package main

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
)

func lockIssuer(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,42))`, id)
	return err
}

// This is the trusted issuer stub, not the unreliable provider response.
// Recording a rejected result is also an irrevocable cancellation fence.
func (a *app) registryIssue(ctx context.Context, provider string, req issueRequest, reject bool) (string, string, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)
	if err = lockIssuer(ctx, tx, req.RequestID); err != nil {
		return "", "", err
	}
	var code, reason, owner, storedProvider, storedSKU, storedOrder string
	err = tx.QueryRow(ctx, `SELECT coalesce(r.code,''),r.reason,r.item_id,r.provider,i.sku,i.order_id FROM issuer_operations r JOIN order_items i ON i.id=r.item_id WHERE r.request_id=$1`, req.RequestID).Scan(&code, &reason, &owner, &storedProvider, &storedSKU, &storedOrder)
	if err == nil {
		if owner != req.ItemID || storedProvider != provider || storedSKU != req.SKU || storedOrder != req.OrderID {
			return "", "", errors.New("request_id payload conflict")
		}
		return code, reason, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", "", err
	}
	var i itemRow
	err = tx.QueryRow(ctx, `SELECT i.id,i.order_id,i.sku,i.provider,i.amount,i.state,i.attempts,i.fallback FROM order_items i JOIN orders o ON o.id=i.order_id WHERE i.id=$1 AND o.paid_at IS NOT NULL`, req.ItemID).Scan(&i.ID, &i.OrderID, &i.SKU, &i.Provider, &i.Amount, &i.State, &i.Attempts, &i.Fallback)
	if err != nil {
		return "", "", err
	}
	if req.OrderID != i.OrderID || req.SKU != i.SKU || provider != i.Provider || req.RequestID != requestID(i) || i.State != "issuing" {
		return "", "", errors.New("request does not match active paid item")
	}
	if reject {
		reason = "unavailable"
	} else {
		// Also adopts a reservation from stage 1 interrupted before local completion.
		err = tx.QueryRow(ctx, `SELECT code FROM inventory_keys WHERE item_id=$1`, i.ID).Scan(&code)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `UPDATE inventory_keys SET status='issued',item_id=$1,order_id=$2,issued_at=clock_timestamp()
    WHERE id=(SELECT id FROM inventory_keys WHERE sku=$3 AND status='available' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING code`, i.ID, i.OrderID, i.SKU).Scan(&code)
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE products SET available_count=greatest(available_count-1,0) WHERE sku=$1`, i.SKU)
			}
		}
		if errors.Is(err, pgx.ErrNoRows) {
			reason = "out_of_stock"
		} else if err != nil {
			return "", "", err
		}
	}
	outcome := "issued"
	if code == "" {
		outcome = "rejected"
	}
	_, err = tx.Exec(ctx, `INSERT INTO issuer_operations(request_id,item_id,provider,code,outcome,reason) VALUES($1,$2,$3,nullif($4,''),$5,$6)`, req.RequestID, i.ID, provider, code, outcome, reason)
	if err != nil {
		return "", "", err
	}
	return code, reason, tx.Commit(ctx)
}

// Atomically look up or cancel: a late request cannot issue after a refund.
func reconcileIssuer(ctx context.Context, tx pgx.Tx, i itemRow) (string, string, error) {
	req := requestID(i)
	if err := lockIssuer(ctx, tx, req); err != nil {
		return "", "", err
	}
	_, err := tx.Exec(ctx, `INSERT INTO issuer_operations(request_id,item_id,provider,outcome,reason)
  VALUES($1,$2,$3,'rejected','cancelled before issue') ON CONFLICT DO NOTHING`, req, i.ID, i.Provider)
	if err != nil {
		return "", "", err
	}
	var code, reason string
	err = tx.QueryRow(ctx, `SELECT coalesce(r.code,''),r.reason FROM issuer_operations r
  LEFT JOIN inventory_keys k ON k.code=r.code
  WHERE r.request_id=$1 AND r.item_id=$2 AND r.provider=$3
    AND (r.outcome='rejected' OR (k.item_id=$2 AND k.sku=$4 AND k.status='issued'))`, req, i.ID, i.Provider, i.SKU).Scan(&code, &reason)
	return code, reason, err
}
