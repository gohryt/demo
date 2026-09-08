package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

type paymentEventReq struct {
	EventID   string `json:"event_id"`
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	Amount    int    `json:"amount"`
	Currency  string `json:"currency"`
	CreatedAt string `json:"created_at"`
}

func (a *app) paymentWebhook(c fiber.Ctx) error {
	var req paymentEventReq
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid json"})
	}
	if req.EventID == "" || req.OrderID == "" || req.Amount <= 0 || req.Currency == "" || (req.Status != "paid" && req.Status != "failed") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid event"})
	}
	occurredAt := time.Now().UTC()
	if req.CreatedAt != "" {
		t, err := time.Parse(time.RFC3339, req.CreatedAt)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid created_at"})
		}
		occurredAt = t
	}

	ctx := c.Context()
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(@id, 0))`, pgx.NamedArgs{"id": req.OrderID}); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO payment_events (event_id, order_id, status, amount, currency, occurred_at)
		VALUES (@event_id, @order_id, @status, @amount, @currency, @occurred_at)
		ON CONFLICT (event_id) DO NOTHING
	`, pgx.NamedArgs{
		"event_id": req.EventID, "order_id": req.OrderID, "status": req.Status,
		"amount": req.Amount, "currency": req.Currency, "occurred_at": occurredAt,
	})
	if err != nil {
		return err
	}

	var appliedAt *time.Time
	var eventStatus, storedOrder, storedCurrency string
	var storedAmount int
	var storedTime time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, applied_at, order_id, amount, currency, occurred_at FROM payment_events WHERE event_id = @event_id FOR UPDATE
	`, pgx.NamedArgs{"event_id": req.EventID}).Scan(&eventStatus, &appliedAt, &storedOrder, &storedAmount, &storedCurrency, &storedTime)
	if err != nil {
		return err
	}
	if storedOrder != req.OrderID || eventStatus != req.Status || storedAmount != req.Amount || storedCurrency != req.Currency || (req.CreatedAt != "" && !storedTime.Equal(occurredAt)) {
		return fiber.NewError(409, "event_id reused with different payload")
	}
	if appliedAt != nil {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return c.JSON(fiber.Map{"accepted": true})
	}

	o, err := getOrderForUpdate(ctx, tx, req.OrderID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return c.JSON(fiber.Map{"accepted": true})
	}
	if err != nil {
		return err
	}

	if o.Amount != req.Amount || o.Currency != req.Currency {
		return fiber.NewError(422, "payment amount or currency mismatch")
	}
	woke, err := applyPayment(ctx, tx, o, eventStatus, req.EventID)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if woke {
		a.ping()
	}
	return c.JSON(fiber.Map{"accepted": true})
}

func applyPendingPayments(ctx context.Context, tx pgx.Tx, o orderRow) (bool, error) {
	rows, err := tx.Query(ctx, `
		SELECT event_id, status FROM payment_events
		WHERE order_id = @order_id AND applied_at IS NULL
		ORDER BY occurred_at, event_id
		FOR UPDATE
	`, pgx.NamedArgs{"order_id": o.ID})
	if err != nil {
		return false, err
	}
	type pending struct {
		EventID string `db:"event_id"`
		Status  string
	}
	list, err := pgx.CollectRows(rows, pgx.RowToStructByName[pending])
	if err != nil {
		return false, err
	}
	woke := false
	for _, p := range list {
		cur, err := getOrderForUpdate(ctx, tx, o.ID)
		if err != nil {
			return false, err
		}
		ok, err := applyPayment(ctx, tx, cur, p.Status, p.EventID)
		if err != nil {
			return false, err
		}
		woke = woke || ok
	}
	return woke, nil
}

func applyPayment(ctx context.Context, tx pgx.Tx, o orderRow, eventStatus, eventID string) (bool, error) {
	var amount int
	var currency string
	if err := tx.QueryRow(ctx, `SELECT amount,currency FROM payment_events WHERE event_id=$1 AND order_id=$2`, eventID, o.ID).Scan(&amount, &currency); err != nil {
		return false, err
	}
	if amount != o.Amount || currency != o.Currency {
		_, err := tx.Exec(ctx, `UPDATE payment_events SET applied_at=now(),rejection_reason='amount or currency mismatch' WHERE event_id=$1`, eventID)
		return false, err
	}
	from := o.Status
	woke := false

	switch eventStatus {
	case "paid":
		if o.Status == "created" || o.Status == "payment_failed" {
			tag, err := tx.Exec(ctx, `
				UPDATE orders
				   SET status = 'paid', paid_at = now(), updated_at = now(), last_error = NULL
				 WHERE id = @id AND status IN ('created', 'payment_failed')
			`, pgx.NamedArgs{"id": o.ID})
			if err != nil {
				return false, err
			}
			if tag.RowsAffected() == 1 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO ledger_entries (order_id, kind, amount, currency)
					VALUES (@id, 'payment_credit', @amount, @currency)
					ON CONFLICT DO NOTHING
				`, pgx.NamedArgs{"id": o.ID, "amount": o.Amount, "currency": o.Currency}); err != nil {
					return false, err
				}
				woke = true
				slog.Info("payment_applied",
					"order_id", o.ID, "event_id", eventID,
					"status_from", from, "status_to", "paid")
			}
		}
	case "failed":
		if o.Status == "created" {
			_, err := tx.Exec(ctx, `
				UPDATE orders
				   SET status = 'payment_failed', updated_at = now(), last_error = 'payment failed'
				 WHERE id = @id AND status = 'created'
			`, pgx.NamedArgs{"id": o.ID})
			if err != nil {
				return false, err
			}
			slog.Info("payment_applied",
				"order_id", o.ID, "event_id", eventID,
				"status_from", from, "status_to", "payment_failed")
		}
	}

	if woke || (from == "created" && eventStatus == "failed") {
		if err := recordHistory(ctx, tx, o.ID, "payment_"+eventStatus); err != nil {
			return false, err
		}
	}
	_, err := tx.Exec(ctx, `
		UPDATE payment_events SET applied_at = now() WHERE event_id = @event_id AND applied_at IS NULL
	`, pgx.NamedArgs{"event_id": eventID})
	return woke, err
}
