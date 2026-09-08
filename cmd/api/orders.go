package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

type itemRequest struct {
	SKU      string `json:"sku"`
	Provider string `json:"provider"`
}

type createOrderReq struct {
	Items []itemRequest `json:"items"`
	SKU   string        `json:"sku"`
	ID    string        `json:"id"`
}

type moneyJSON struct {
	RefundSent     int `json:"refund_sent"`
	RefundUnposted int `json:"refund_unposted"`
	Paid           int `json:"paid"`
	Delivered      int `json:"delivered"`
	Refunded       int `json:"refunded"`
	Pending        int `json:"pending"`
}

type orderJSON struct {
	Items    []itemJSON `json:"items"`
	Money    moneyJSON  `json:"money"`
	ID       string     `json:"id"`
	SKU      string     `json:"sku"`
	Amount   int        `json:"amount"`
	Currency string     `json:"currency"`
	Status   string     `json:"status"`
	Code     string     `json:"code,omitempty"`
	Provider string     `json:"provider,omitempty"`
	Error    string     `json:"error,omitempty"`
}

type orderRow struct {
	ID             string
	SKU            string
	Amount         int
	Currency       string
	Status         string
	IssuedCode     *string    `db:"issued_code"`
	IssuedProvider *string    `db:"issued_provider"`
	LastError      *string    `db:"last_error"`
	FailCount      int        `db:"fail_count"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
	PaidAt         *time.Time `db:"paid_at"`
	DeliveredAt    *time.Time `db:"delivered_at"`
}

func (a *app) createOrder(c fiber.Ctx) error {
	var req createOrderReq
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid json"})
	}
	req.SKU = strings.TrimSpace(req.SKU)
	req.ID = strings.TrimSpace(req.ID)
	legacy := len(req.Items) == 0
	if req.SKU != "" && !legacy {
		return fiber.NewError(400, "use sku or items, not both")
	}
	if legacy {
		req.Items = []itemRequest{{SKU: req.SKU, Provider: "a"}}
	}
	if len(req.Items) > 100 {
		return fiber.NewError(400, "at most 100 items")
	}
	for i := range req.Items {
		req.Items[i].SKU = strings.TrimSpace(req.Items[i].SKU)
		if req.Items[i].Provider == "" {
			req.Items[i].Provider = "a"
		}
		if req.Items[i].SKU == "" || (req.Items[i].Provider != "a" && req.Items[i].Provider != "b") {
			return fiber.NewError(400, "valid sku and provider required")
		}
	}
	if req.ID == "" {
		req.ID = newID("ord_")
	}

	ctx := c.Context()
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(@id, 0))`, pgx.NamedArgs{"id": req.ID}); err != nil {
		return err
	}

	// The cart is the idempotency payload; prices remain frozen at creation.
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM orders WHERE id=$1)`, req.ID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		rows, e := tx.Query(ctx, `SELECT sku, CASE WHEN fallback THEN 'a' ELSE provider END AS provider FROM order_items WHERE order_id=$1 ORDER BY position`, req.ID)
		if e != nil {
			return e
		}
		original, e := pgx.CollectRows(rows, pgx.RowToStructByName[itemRequest])
		if e != nil {
			return e
		}
		same := len(original) == len(req.Items)
		for i := range original {
			if !same || original[i] != req.Items[i] {
				same = false
				break
			}
		}
		var wasLegacy bool
		if e = tx.QueryRow(ctx, `SELECT fallback FROM order_items WHERE order_id=$1 LIMIT 1`, req.ID).Scan(&wasLegacy); e != nil {
			return e
		}
		if !same || wasLegacy != legacy {
			return fiber.NewError(409, "order id reused with another cart")
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
		return a.getOrderResponse(c, req.ID, 200)
	}
	prices := make([]int, len(req.Items))
	total := 0
	currency := ""
	for i, it := range req.Items {
		var curr string
		err = tx.QueryRow(ctx, `SELECT price,currency FROM products WHERE sku=$1`, it.SKU).Scan(&prices[i], &curr)
		if errors.Is(err, pgx.ErrNoRows) {
			return fiber.NewError(404, "unknown sku")
		}
		if err != nil {
			return err
		}
		if prices[i] <= 0 || (currency != "" && currency != curr) {
			return fiber.NewError(400, "invalid price or mixed currencies")
		}
		currency = curr
		total += prices[i]
		if total > 2147483647 {
			return fiber.NewError(400, "order total too large")
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO orders(id,sku,amount,currency,status) VALUES($1,$2,$3,$4,'created')`, req.ID, req.Items[0].SKU, total, currency)
	if err != nil {
		return err
	}
	for i, it := range req.Items {
		_, err = tx.Exec(ctx, `INSERT INTO order_items(id,order_id,position,sku,provider,fallback,amount,state)
   VALUES($1,$2,$3,$4,$5,$6,$7,'pending')`, newID("item_"), req.ID, i+1, it.SKU, it.Provider, legacy, prices[i])
		if err != nil {
			return err
		}
	}
	if err = recordHistory(ctx, tx, req.ID, "created"); err != nil {
		return err
	}

	o, err := getOrderForUpdate(ctx, tx, req.ID)
	if err != nil {
		return err
	}
	applied, err := applyPendingPayments(ctx, tx, o)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if applied {
		a.ping()
	}

	return a.getOrderResponse(c, req.ID, fiber.StatusCreated)
}

func (a *app) getOrder(c fiber.Ctx) error {
	return a.getOrderResponse(c, strings.Clone(c.Params("id")), 200)
}

func (a *app) getOrderResponse(c fiber.Ctx, id string, status int) error {
	var raw []byte
	var err error
	if at := c.Query("at"); at != "" {
		instant, e := time.Parse(time.RFC3339Nano, at)
		if e != nil {
			return fiber.NewError(400, "invalid at: use RFC3339")
		}
		err = a.pool.QueryRow(c.Context(), `SELECT snapshot FROM order_history WHERE order_id=$1 AND recorded_at<=$2 ORDER BY recorded_at DESC,id DESC LIMIT 1`, id, instant).Scan(&raw)
	} else {
		err = a.pool.QueryRow(c.Context(), `SELECT order_snapshot(id) FROM orders WHERE id=$1`, id).Scan(&raw)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fiber.NewError(404, "order not found at this time")
	}
	if err != nil {
		return err
	}
	c.Set("Content-Type", "application/json")
	return c.Status(status).Send(raw)
}

func getOrderForUpdate(ctx context.Context, tx pgx.Tx, id string) (orderRow, error) {
	rows, err := tx.Query(ctx, `SELECT * FROM orders WHERE id = @id FOR UPDATE`, pgx.NamedArgs{"id": id})
	if err != nil {
		return orderRow{}, err
	}
	return pgx.CollectOneRow(rows, pgx.RowToStructByName[orderRow])
}

func newID(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}
