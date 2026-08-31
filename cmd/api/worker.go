package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

func (a *app) runWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		for a.processOne(ctx) {
		}
		select {
		case <-ctx.Done():
			return
		case <-a.wake:
		case <-ticker.C:
		}
	}
}

func (a *app) processOne(ctx context.Context) bool {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		slog.Error("worker begin", "err", err)
		return false
	}
	defer tx.Rollback(ctx)

	var id string
	err = tx.QueryRow(ctx, `
		SELECT id FROM orders
		 WHERE status IN ('paid', 'delivering', 'out_of_stock', 'delivery_failed')
		   AND updated_at <= now()
		 ORDER BY updated_at
		 FOR UPDATE SKIP LOCKED
		 LIMIT 1
	`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		slog.Error("worker pick", "err", err)
		return false
	}

	o, err := getOrderForUpdate(ctx, tx, id)
	if err != nil {
		slog.Error("worker load", "err", err)
		return false
	}
	if o.Status == "delivered" || o.Status == "created" || o.Status == "payment_failed" {
		return false
	}

	_, err = tx.Exec(ctx, `
		UPDATE orders SET status = 'delivering', updated_at = now()
		 WHERE id = @id
	`, pgx.NamedArgs{"id": o.ID})
	if err != nil {
		slog.Error("worker delivering", "err", err)
		return false
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("worker commit", "err", err)
		return false
	}

	reqA := "req_" + o.ID + "_a"
	code, explicit, reason, err := a.issue(ctx, "a", reqA, o.SKU, o.ID)
	if err == nil && code != "" {
		if err := a.completeDelivery(ctx, o, "a", reqA, code); err != nil {
			slog.Error("complete", "order_id", o.ID, "err", err)
		}
		return true
	}
	if err != nil && !explicit {
		slog.Info("delivery_timeout", "order_id", o.ID, "request_id", reqA, "provider", "a")
		_ = a.backoff(ctx, o, "timeout a")
		return true
	}

	slog.Info("delivery_fallback", "order_id", o.ID, "request_id", reqA, "provider", "a", "reason", reason)
	reqB := "req_" + o.ID + "_b"
	code, explicitB, reasonB, errB := a.issue(ctx, "b", reqB, o.SKU, o.ID)
	if errB == nil && code != "" {
		if err := a.completeDelivery(ctx, o, "b", reqB, code); err != nil {
			slog.Error("complete", "order_id", o.ID, "err", err)
		}
		return true
	}
	if errB != nil && !explicitB {
		slog.Info("delivery_timeout", "order_id", o.ID, "request_id", reqB, "provider", "b")
		_ = a.backoff(ctx, o, "timeout b")
		return true
	}

	status := "delivery_failed"
	if reason == "out_of_stock" || reasonB == "out_of_stock" {
		status = "out_of_stock"
		slog.Info("delivery_out_of_stock", "order_id", o.ID)
	}
	msg := reason
	if msg == "" {
		msg = reasonB
	}
	if msg == "" {
		msg = "providers failed"
	}
	_, _ = a.pool.Exec(ctx, `
		UPDATE orders
		   SET status = @status,
		       last_error = @err,
		       fail_count = fail_count + 1,
		       updated_at = now() + @delay
		 WHERE id = @id AND status = 'delivering'
	`, pgx.NamedArgs{"status": status, "err": msg, "id": o.ID, "delay": backoffDuration(o.FailCount)})
	return true
}

func (a *app) completeDelivery(ctx context.Context, o orderRow, provider, requestID, code string) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	cur, err := getOrderForUpdate(ctx, tx, o.ID)
	if err != nil {
		return err
	}
	if cur.Status == "delivered" {
		return tx.Commit(ctx)
	}

	tag, err := tx.Exec(ctx, `
		UPDATE orders
		   SET status = 'delivered',
		       issued_code = @code,
		       issued_provider = @provider,
		       delivered_at = now(),
		       updated_at = now(),
		       last_error = NULL
		 WHERE id = @id AND status IN ('delivering', 'paid', 'out_of_stock', 'delivery_failed')
	`, pgx.NamedArgs{"code": code, "provider": provider, "id": o.ID})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (order_id, kind, amount, currency)
		VALUES (@id, 'delivery_debit', @amount, @currency)
		ON CONFLICT (order_id, kind) DO NOTHING
	`, pgx.NamedArgs{"id": o.ID, "amount": -o.Amount, "currency": o.Currency}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	slog.Info("delivery_ok",
		"order_id", o.ID, "request_id", requestID, "provider", provider)
	return nil
}

func backoffDuration(failCount int) time.Duration {
	shift := failCount
	if shift > 6 {
		shift = 6
	}
	d := 100 * time.Millisecond * time.Duration(1<<shift)
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

func (a *app) backoff(ctx context.Context, o orderRow, reason string) error {
	d := backoffDuration(o.FailCount)
	_, err := a.pool.Exec(ctx, `
		UPDATE orders
		   SET fail_count = fail_count + 1,
		       last_error = @reason,
		       updated_at = now() + @delay
		 WHERE id = @id
	`, pgx.NamedArgs{"reason": reason, "delay": d, "id": o.ID})
	return err
}

type issueRequest struct {
	RequestID string `json:"request_id"`
	SKU       string `json:"sku"`
	OrderID   string `json:"order_id"`
}

type issueResponse struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	Code      string `json:"code"`
	Reason    string `json:"reason"`
}

func (a *app) issue(ctx context.Context, provider, requestID, sku, orderID string) (code string, explicit bool, reason string, err error) {
	body, _ := json.Marshal(issueRequest{RequestID: requestID, SKU: sku, OrderID: orderID})
	pctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(pctx, http.MethodPost, a.selfURL+"/stub/provider-"+provider+"/issue", bytes.NewReader(body))
	if err != nil {
		return "", false, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return "", false, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed issueResponse
	_ = json.Unmarshal(raw, &parsed)
	if resp.StatusCode == http.StatusOK && parsed.Code != "" {
		return parsed.Code, false, "", nil
	}
	reason = parsed.Reason
	if reason == "" {
		reason = http.StatusText(resp.StatusCode)
	}
	return "", true, reason, errors.New(reason)
}
