package main

import (
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

type reconItem struct {
	ID     string     `json:"id"`
	Status string     `json:"status"`
	PaidAt *time.Time `json:"paid_at,omitempty" db:"paid_at"`
}

func (a *app) reconciliation(c fiber.Ctx) error {
	ctx := c.Context()

	rows, err := a.pool.Query(ctx, `
		SELECT id, status, paid_at FROM orders
		 WHERE status IN ('paid', 'delivering', 'out_of_stock', 'delivery_failed')
		 ORDER BY paid_at
	`)
	if err != nil {
		return err
	}
	paidNotDelivered, err := pgx.CollectRows(rows, pgx.RowToStructByName[reconItem])
	if err != nil {
		return err
	}

	rows, err = a.pool.Query(ctx, `
		SELECT o.id, o.status, o.paid_at
		  FROM orders o
		 WHERE o.status IN ('delivered','partially_refunded')
		   AND NOT EXISTS (
		         SELECT 1 FROM payment_events e
		          WHERE e.order_id = o.id AND e.status = 'paid' AND e.applied_at IS NOT NULL AND e.rejection_reason IS NULL
		       )
	`)
	if err != nil {
		return err
	}
	deliveredNotPaid, err := pgx.CollectRows(rows, pgx.RowToStructByName[reconItem])
	if err != nil {
		return err
	}

	if paidNotDelivered == nil {
		paidNotDelivered = []reconItem{}
	}
	if deliveredNotPaid == nil {
		deliveredNotPaid = []reconItem{}
	}
	rows, err = a.pool.Query(ctx, `SELECT id,status,order_snapshot(id)->'money' AS money,
  CASE WHEN status IN ('delivered','partially_refunded','refunded')
  THEN (SELECT coalesce(sum(amount),0)=0 FROM ledger_entries WHERE order_id=o.id)
  ELSE (SELECT coalesce(sum(amount),0)>=0 FROM ledger_entries WHERE order_id=o.id) END AS balanced
  FROM orders o ORDER BY created_at,id`)
	if err != nil {
		return err
	}
	balances, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return err
	}
	rows, err = a.pool.Query(ctx, `SELECT * FROM provider_discrepancies ORDER BY detected_at,request_id`)
	if err != nil {
		return err
	}
	discrepancies, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{
		"balances": balances, "provider_discrepancies": discrepancies,
		"paid_not_delivered": paidNotDelivered,
		"delivered_not_paid": deliveredNotPaid,
	})
}

func (a *app) retryOrder(c fiber.Ctx) error {
	id := strings.Clone(c.Params("id"))
	ctx := c.Context()
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM orders WHERE id=$1 FOR UPDATE`, id).Scan(&status)
	if err == pgx.ErrNoRows {
		return fiber.NewError(404, "not found")
	}
	if err != nil {
		return err
	}
	if status == "created" || status == "payment_failed" {
		return fiber.NewError(409, "order is not paid")
	}
	// Terminal retries are successful no-ops; never reset an active lease.
	if _, err = tx.Exec(ctx, `UPDATE order_items SET next_attempt_at=clock_timestamp() WHERE order_id=$1 AND state IN ('pending','issuing','refund_pending') AND lease_token IS NULL`, id); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	a.ping()
	return c.JSON(fiber.Map{"ok": true})
}
