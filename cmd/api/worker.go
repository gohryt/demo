package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

type itemJSON struct {
	ID       string  `json:"id"`
	SKU      string  `json:"sku"`
	Provider string  `json:"provider"`
	Amount   int     `json:"amount"`
	State    string  `json:"state"`
	Code     *string `json:"code"`
	Attempts int     `json:"attempts"`
	Error    *string `json:"error"`
}
type itemRow struct {
	ID, OrderID, SKU, Provider, State, Token string
	Amount, Attempts                         int
	Fallback                                 bool
}

func (a *app) runWorker(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for ctx.Err() == nil && a.processOne(ctx) {
		}
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
		case <-ticker.C:
		}
	}
}

func (a *app) claim(ctx context.Context) (itemRow, error) {
	var i itemRow
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return i, err
	}
	defer tx.Rollback(ctx)
	var oid string
	err = tx.QueryRow(ctx, `SELECT o.id FROM orders o WHERE o.paid_at IS NOT NULL
  AND o.status NOT IN ('delivered','partially_refunded','refunded')
  AND EXISTS(SELECT 1 FROM order_items i JOIN provider_settings p ON p.provider=i.provider
   WHERE i.order_id=o.id AND i.state IN ('pending','issuing','refund_pending')
   AND i.next_attempt_at<=clock_timestamp() AND (i.lease_until IS NULL OR i.lease_until<clock_timestamp())
   AND (i.state='refund_pending' OR EXISTS(SELECT 1 FROM issuer_operations r WHERE r.request_id='req_'||i.id||'_'||i.provider||'_'||i.attempts)
    OR (SELECT count(*) FROM provider_calls pc WHERE pc.provider=i.provider AND (pc.admitted_at>clock_timestamp()-interval '1 minute' OR (pc.admitted_at IS NULL AND pc.valid_until>clock_timestamp())))<p.requests_per_minute))
  ORDER BY o.paid_at,o.id FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&oid)
	if err != nil {
		return i, err
	}
	err = tx.QueryRow(ctx, `SELECT i.id,i.order_id,i.sku,i.provider,i.amount,i.state,i.attempts,i.fallback
  FROM order_items i JOIN provider_settings p ON p.provider=i.provider
  WHERE i.order_id=$1 AND i.state IN ('pending','issuing','refund_pending')
  AND i.next_attempt_at<=clock_timestamp() AND (i.lease_until IS NULL OR i.lease_until<clock_timestamp())
  AND (i.state='refund_pending' OR EXISTS(SELECT 1 FROM issuer_operations r WHERE r.request_id='req_'||i.id||'_'||i.provider||'_'||i.attempts)
   OR (SELECT count(*) FROM provider_calls pc WHERE pc.provider=i.provider AND (pc.admitted_at>clock_timestamp()-interval '1 minute' OR (pc.admitted_at IS NULL AND pc.valid_until>clock_timestamp())))<p.requests_per_minute)
  ORDER BY i.position FOR UPDATE OF i SKIP LOCKED LIMIT 1`, oid).Scan(&i.ID, &i.OrderID, &i.SKU, &i.Provider, &i.Amount, &i.State, &i.Attempts, &i.Fallback)
	if err != nil {
		return i, err
	}
	i.Token = newID("lease_")
	if i.State != "refund_pending" {
		i.State = "issuing"
	}
	_, err = tx.Exec(ctx, `UPDATE order_items SET state=$2,lease_token=$3,lease_until=clock_timestamp()+$4::interval WHERE id=$1`, i.ID, i.State, i.Token, (a.timeout + 5*time.Second).String())
	if err != nil {
		return i, err
	}
	if _, err = tx.Exec(ctx, `UPDATE orders SET status='delivering',updated_at=clock_timestamp() WHERE id=$1`, oid); err != nil {
		return i, err
	}
	if err = recordHistory(ctx, tx, oid, "item_claimed"); err != nil {
		return i, err
	}
	return i, tx.Commit(ctx)
}

func (a *app) processOne(ctx context.Context) bool {
	i, err := a.claim(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		slog.Error("claim", "err", err)
		return false
	}
	if i.State == "refund_pending" {
		err = a.refund(ctx, i)
		if err == nil {
			err = a.finishRefund(ctx, i)
		}
	} else {
		// On restart, an already persisted issuer result needs no supplier quota.
		var exists bool
		err = a.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM issuer_operations WHERE request_id=$1)`, requestID(i)).Scan(&exists)
		if err == nil {
			var response issueResponse
			var limited bool
			if !exists {
				response, limited = a.issue(ctx, i)
			}
			err = a.finishIssue(ctx, i, response, limited)
		}
	}
	if err != nil {
		slog.Error("process item", "item_id", i.ID, "err", err)
	}
	return true
}

func (a *app) issue(ctx context.Context, i itemRow) (issueResponse, bool) {
	permit, err := a.reserveProvider(ctx, i.Provider, requestID(i))
	if err != nil {
		return issueResponse{Reason: err.Error()}, true
	}
	if permit == 0 {
		return issueResponse{Reason: "rate_limited"}, true
	}
	body, _ := json.Marshal(issueRequest{PermitID: permit, RequestID: requestID(i), SKU: i.SKU, OrderID: i.OrderID, ItemID: i.ID})
	pctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodPost, a.selfURL+"/stub/provider-"+i.Provider+"/issue", bytes.NewReader(body))
	if err != nil {
		return issueResponse{Reason: err.Error()}, false
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := a.client.Do(req)
	if err != nil {
		return issueResponse{Reason: err.Error()}, false
	}
	defer res.Body.Close()
	var response issueResponse
	if err = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&response); err != nil {
		response.Reason = "invalid supplier response"
	}
	if res.StatusCode != 200 && response.Reason == "" {
		response.Reason = res.Status
	}
	if res.StatusCode == 200 && (response.Status != "ok" || response.Code == "" || response.RequestID != requestID(i)) {
		response.Reason = "invalid supplier success response"
	}
	return response, res.StatusCode == 429
}

func lockItem(ctx context.Context, tx pgx.Tx, i itemRow) (bool, error) {
	var id string
	if err := tx.QueryRow(ctx, `SELECT id FROM orders WHERE id=$1 FOR UPDATE`, i.OrderID).Scan(&id); err != nil {
		return false, err
	}
	var token *string
	var state string
	err := tx.QueryRow(ctx, `SELECT lease_token,state FROM order_items WHERE id=$1 FOR UPDATE`, i.ID).Scan(&token, &state)
	return token != nil && *token == i.Token && state == i.State, err
}

func (a *app) finishIssue(ctx context.Context, i itemRow, response issueResponse, limited bool) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	owned, err := lockItem(ctx, tx, i)
	if err != nil {
		return err
	}
	if !owned {
		return nil
	}
	if limited {
		_, err = tx.Exec(ctx, `UPDATE order_items SET lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()+interval '1 second' WHERE id=$1`, i.ID)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	code, reason, err := reconcileIssuer(ctx, tx, i)
	if err != nil {
		return err
	}
	if response.Reason != "" || (response.Code != "" && (response.Code != code || response.RequestID != requestID(i))) {
		if response.Code != "" && response.Code != code {
			response.Reason = "reported code does not match issuer registry"
		}
		resolution := "confirmed_rejection"
		if code != "" {
			resolution = "recovered_verified_code"
		}
		_, err = tx.Exec(ctx, `INSERT INTO provider_discrepancies(request_id,reported_code,canonical_code,reason,resolution)
   VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, requestID(i), response.Code, code, fmt.Sprintf("untrusted response: %s; registry: %s", response.Reason, reason), resolution)
		if err != nil {
			return err
		}
	}
	if code != "" {
		_, err = tx.Exec(ctx, `UPDATE order_items SET state='delivered',code=$2,last_error=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1`, i.ID, code)
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO ledger_entries(order_id,item_id,kind,amount,currency)
   SELECT id,$2,'delivery_debit',-$3::int,currency FROM orders WHERE id=$1`, i.OrderID, i.ID, i.Amount)
		}
	} else {
		state := "pending"
		provider := i.Provider
		attempts := i.Attempts + 1
		if i.Fallback && provider == "a" {
			provider = "b"
			attempts = 0
		} else if attempts >= 3 {
			state = "refund_pending"
		}
		_, err = tx.Exec(ctx, `UPDATE order_items SET state=$2,provider=$3,attempts=$4,last_error=$5,
   lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()+$6::interval WHERE id=$1`, i.ID, state, provider, attempts, reason, backoffDuration(i.Attempts).String())
	}
	if err != nil {
		return err
	}
	if err = refreshOrder(ctx, tx, i.OrderID, reason); err != nil {
		return err
	}
	kind := "item_rejected"
	if code != "" {
		kind = "item_delivered"
	}
	if err = recordHistory(ctx, tx, i.OrderID, kind); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Durable payment stub. Its commit is deliberately separate from finishRefund:
// a crash in between is recovered by the same refund request id.
func (a *app) refund(ctx context.Context, i itemRow) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var orderID string
	if err = tx.QueryRow(ctx, `SELECT id FROM orders WHERE id=$1 FOR UPDATE`, i.OrderID).Scan(&orderID); err != nil {
		return err
	}
	var state, currency string
	var amount int
	err = tx.QueryRow(ctx, `SELECT i.state,i.amount,o.currency FROM order_items i JOIN orders o ON o.id=i.order_id WHERE i.id=$1 FOR UPDATE OF i`, i.ID).Scan(&state, &amount, &currency)
	if err != nil {
		return err
	}
	if state != "refund_pending" && state != "refunded" {
		return errors.New("item not refundable")
	}
	tag, err := tx.Exec(ctx, `INSERT INTO refund_receipts(request_id,item_id,amount,currency) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, "refund_"+i.ID, i.ID, amount, currency)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		if err = recordHistory(ctx, tx, i.OrderID, "refund_sent"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (a *app) finishRefund(ctx context.Context, i itemRow) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	owned, err := lockItem(ctx, tx, i)
	if err != nil {
		return err
	}
	if !owned {
		return nil
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ledger_entries(order_id,item_id,kind,amount,currency)
  SELECT $1,r.item_id,'refund_debit',-r.amount,r.currency FROM refund_receipts r
  JOIN orders o ON o.id=$1 WHERE r.item_id=$2 AND r.amount=$3 AND r.currency=o.currency`, i.OrderID, i.ID, i.Amount)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("missing refund receipt")
	}
	if _, err = tx.Exec(ctx, `UPDATE order_items SET state='refunded',lease_token=NULL,lease_until=NULL WHERE id=$1`, i.ID); err != nil {
		return err
	}
	if err = refreshOrder(ctx, tx, i.OrderID, ""); err != nil {
		return err
	}
	if err = recordHistory(ctx, tx, i.OrderID, "item_refunded"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func refreshOrder(ctx context.Context, tx pgx.Tx, id, reason string) error {
	var total, delivered, refunded, balance int
	if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE state='delivered'),count(*) FILTER(WHERE state='refunded') FROM order_items WHERE order_id=$1`, id).Scan(&total, &delivered, &refunded); err != nil {
		return err
	}
	status := "delivering"
	if delivered+refunded == total {
		if err := tx.QueryRow(ctx, `SELECT coalesce(sum(amount),0) FROM ledger_entries WHERE order_id=$1`, id).Scan(&balance); err != nil {
			return err
		}
		if balance != 0 {
			return errors.New("terminal money invariant violated")
		}
		switch {
		case refunded == 0:
			status = "delivered"
		case delivered == 0:
			status = "refunded"
		default:
			status = "partially_refunded"
		}
		reason = ""
	} else if reason == "out_of_stock" {
		status = "out_of_stock"
	} else if reason != "" {
		status = "delivery_failed"
	}
	_, err := tx.Exec(ctx, `UPDATE orders SET status=$2,last_error=nullif($3,''),updated_at=clock_timestamp(),
  delivered_at=CASE WHEN $2 IN ('delivered','partially_refunded','refunded') THEN clock_timestamp() ELSE delivered_at END,
  issued_code=CASE WHEN $4=1 THEN (SELECT code FROM order_items WHERE order_id=$1 LIMIT 1) ELSE NULL END,
  issued_provider=CASE WHEN $4=1 AND $5=1 THEN (SELECT provider FROM order_items WHERE order_id=$1 LIMIT 1) ELSE NULL END WHERE id=$1`, id, status, reason, total, delivered)
	return err
}

func backoffDuration(failCount int) time.Duration {
	if failCount > 5 {
		failCount = 5
	}
	return time.Second * time.Duration(1<<failCount)
}
