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
		 WHERE o.status = 'delivered'
		   AND NOT EXISTS (
		         SELECT 1 FROM payment_events e
		          WHERE e.order_id = o.id AND e.status = 'paid' AND e.applied_at IS NOT NULL
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
	return c.JSON(fiber.Map{
		"paid_not_delivered": paidNotDelivered,
		"delivered_not_paid": deliveredNotPaid,
	})
}

func (a *app) retryOrder(c fiber.Ctx) error {
	id := strings.Clone(c.Params("id"))
	tag, err := a.pool.Exec(c.Context(), `
		UPDATE orders
		   SET updated_at = now()
		 WHERE id = @id
		   AND status IN ('paid', 'delivering', 'out_of_stock', 'delivery_failed')
	`, pgx.NamedArgs{"id": id})
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "not retryable"})
	}
	a.ping()
	return c.JSON(fiber.Map{"ok": true})
}
