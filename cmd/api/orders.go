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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type createOrderReq struct {
	SKU string `json:"sku"`
	ID  string `json:"id"`
}

type orderJSON struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	Amount   int    `json:"amount"`
	Currency string `json:"currency"`
	Status   string `json:"status"`
	Code     string `json:"code,omitempty"`
	Provider string `json:"provider,omitempty"`
	Error    string `json:"error,omitempty"`
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

func (o orderRow) json() orderJSON {
	out := orderJSON{
		ID:       o.ID,
		SKU:      o.SKU,
		Amount:   o.Amount,
		Currency: o.Currency,
		Status:   o.Status,
	}
	if o.IssuedCode != nil {
		out.Code = *o.IssuedCode
	}
	if o.IssuedProvider != nil {
		out.Provider = *o.IssuedProvider
	}
	if o.LastError != nil {
		out.Error = *o.LastError
	}
	return out
}

func (a *app) createOrder(c fiber.Ctx) error {
	var req createOrderReq
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid json"})
	}
	req.SKU = strings.TrimSpace(req.SKU)
	req.ID = strings.TrimSpace(req.ID)
	if req.SKU == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "sku required"})
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

	var product struct {
		Price    int
		Currency string
	}
	err = tx.QueryRow(ctx, `SELECT price, currency FROM products WHERE sku = @sku`, pgx.NamedArgs{"sku": req.SKU}).
		Scan(&product.Price, &product.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "unknown sku"})
	}
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, sku, amount, currency, status)
		VALUES (@id, @sku, @amount, @currency, 'created')
	`, pgx.NamedArgs{
		"id": req.ID, "sku": req.SKU, "amount": product.Price, "currency": product.Currency,
	})
	if isUniqueViolation(err) {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "order id exists"})
	}
	if err != nil {
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

	o, err = getOrder(ctx, a.pool, req.ID)
	if err != nil {
		return err
	}
	return c.Status(fiber.StatusCreated).JSON(o.json())
}

func (a *app) getOrder(c fiber.Ctx) error {
	id := strings.Clone(c.Params("id"))
	o, err := getOrder(c.Context(), a.pool, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "not found"})
	}
	if err != nil {
		return err
	}
	return c.JSON(o.json())
}

func getOrder(ctx context.Context, pool *pgxpool.Pool, id string) (orderRow, error) {
	rows, err := pool.Query(ctx, `SELECT * FROM orders WHERE id = @id`, pgx.NamedArgs{"id": id})
	if err != nil {
		return orderRow{}, err
	}
	return pgx.CollectOneRow(rows, pgx.RowToStructByName[orderRow])
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
