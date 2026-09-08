package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

type issueRequest struct {
	PermitID  int64  `json:"permit_id"`
	RequestID string `json:"request_id"`
	SKU       string `json:"sku"`
	OrderID   string `json:"order_id"`
	ItemID    string `json:"item_id"`
}
type issueResponse struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	Code      string `json:"code"`
	Reason    string `json:"reason"`
}

func requestID(i itemRow) string { return fmt.Sprintf("req_%s_%s_%d", i.ID, i.Provider, i.Attempts) }

// Reserve before HTTP; an unconsumed permit counts until expiry. Consumption
// starts a fresh 60-second accounting interval. Late arrivals cannot consume an
// expired permit. This closes the reserve -> process pause -> delayed send gap.
func (a *app) reserveProvider(ctx context.Context, provider, request string) (int64, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var limit, n int
	if err = tx.QueryRow(ctx, `SELECT requests_per_minute FROM provider_settings WHERE provider=$1 FOR UPDATE`, provider).Scan(&limit); err != nil {
		return 0, err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM provider_calls WHERE provider=$1 AND
  (admitted_at>clock_timestamp()-interval '1 minute' OR (admitted_at IS NULL AND valid_until>clock_timestamp()))`, provider).Scan(&n); err != nil {
		return 0, err
	}
	if n >= limit {
		return 0, tx.Commit(ctx)
	}
	var permit int64
	err = tx.QueryRow(ctx, `INSERT INTO provider_calls(provider,request_id,valid_until) VALUES($1,$2,clock_timestamp()+$3::interval) RETURNING id`, provider, request, (a.timeout + 5*time.Second).String()).Scan(&permit)
	if err != nil {
		return 0, err
	}
	return permit, tx.Commit(ctx)
}

func (a *app) consumePermit(ctx context.Context, provider string, req issueRequest) (string, bool, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)
	var mode string
	var limit, n int
	if err = tx.QueryRow(ctx, `SELECT mode,requests_per_minute FROM provider_settings WHERE provider=$1 FOR UPDATE`, provider).Scan(&mode, &limit); err != nil {
		return "", false, err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM provider_calls WHERE provider=$1 AND admitted_at>clock_timestamp()-interval '1 minute'`, provider).Scan(&n); err != nil {
		return "", false, err
	}
	if n >= limit {
		return mode, false, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `UPDATE provider_calls SET admitted_at=clock_timestamp() WHERE id=$1 AND provider=$2 AND request_id=$3 AND admitted_at IS NULL AND valid_until>clock_timestamp()`, req.PermitID, provider, req.RequestID)
	if err != nil {
		return "", false, err
	}
	return mode, tag.RowsAffected() == 1, tx.Commit(ctx)
}

func (a *app) stubIssue(provider string) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req issueRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil || req.RequestID == "" || req.ItemID == "" || req.SKU == "" || req.OrderID == "" {
			return fiber.NewError(400, "invalid issue request")
		}
		mode, ok, err := a.consumePermit(c.Context(), provider, req)
		if err != nil {
			return err
		}
		if !ok {
			return c.Status(429).JSON(issueResponse{Status: "error", Reason: "rate_limited"})
		}
		er, tr := a.errA, a.toA
		if provider == "b" {
			er, tr = a.errB, a.toB
		}
		if er > 0 && rand.Float64() < er {
			mode = "unavailable"
		}
		if tr > 0 && rand.Float64() < tr {
			mode = "timeout_after_issue"
		}
		code, reason, err := a.registryIssue(c.Context(), provider, req, mode == "unavailable")
		if err != nil {
			return err
		}
		if code == "" {
			return c.Status(503).JSON(issueResponse{Status: "error", RequestID: req.RequestID, Reason: reason})
		}
		switch mode {
		case "error_after_issue":
			return c.Status(500).JSON(issueResponse{Status: "error", Reason: "lost acknowledgment"})
		case "timeout_after_issue":
			time.Sleep(a.timeout + 200*time.Millisecond)
		case "duplicate":
			// Deliberately return another buyer's code, without changing the independent registry.
			var other string
			err = a.pool.QueryRow(c.Context(), `SELECT code FROM inventory_keys WHERE status='issued' AND item_id<>$1 ORDER BY id LIMIT 1`, req.ItemID).Scan(&other)
			if err == nil {
				code = other
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		case "wrong_sku":
			var other string
			err = a.pool.QueryRow(c.Context(), `SELECT code FROM inventory_keys WHERE sku<>$1 ORDER BY id LIMIT 1`, req.SKU).Scan(&other)
			if err == nil {
				code = other
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		return c.JSON(issueResponse{Status: "ok", RequestID: req.RequestID, Code: code})
	}
}

func (a *app) configureProvider(c fiber.Ctx) error {
	var req struct {
		Mode  string `json:"mode"`
		Limit int    `json:"requests_per_minute"`
	}
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return fiber.NewError(400, "invalid json")
	}
	switch req.Mode {
	case "normal", "unavailable", "duplicate", "wrong_sku", "error_after_issue", "timeout_after_issue":
	default:
		return fiber.NewError(400, "invalid mode")
	}
	if req.Limit <= 0 || req.Limit > 100000 {
		return fiber.NewError(400, "invalid requests_per_minute")
	}
	tag, err := a.pool.Exec(c.Context(), `UPDATE provider_settings SET mode=$1,requests_per_minute=$2 WHERE provider=$3`, req.Mode, req.Limit, c.Params("provider"))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fiber.NewError(404, "unknown provider")
	}
	a.ping()
	return c.JSON(fiber.Map{"ok": true})
}
