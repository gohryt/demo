package main

import (
	"encoding/json"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

func (a *app) lockRequest(id string) func() {
	v, _ := a.reqLocks.LoadOrStore(id, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (a *app) stubIssue(provider string) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req issueRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil || req.RequestID == "" || req.SKU == "" || req.OrderID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(issueResponse{Status: "error", Reason: "bad_request"})
		}

		errRate, toRate := a.errA, a.toA
		if provider == "b" {
			errRate, toRate = a.errB, a.toB
		}

		code, hang, status, err := func() (string, bool, int, error) {
			unlock := a.lockRequest(req.RequestID)
			defer unlock()

			if v, ok := a.issued.Load(req.RequestID); ok {
				return v.(string), false, fiber.StatusOK, nil
			}
			if errRate > 0 && rand.Float64() < errRate {
				return "", false, fiber.StatusInternalServerError, nil
			}

			ctx := c.Context()
			var code string
			claimed := true
			err := a.pool.QueryRow(ctx, `
				UPDATE inventory_keys k
				   SET status = 'issued', order_id = @order_id, issued_at = now()
				  FROM (
				        SELECT id FROM inventory_keys
				         WHERE sku = @sku AND status = 'available'
				         FOR UPDATE SKIP LOCKED
				         LIMIT 1
				       ) t
				 WHERE k.id = t.id
				 RETURNING k.code
			`, pgx.NamedArgs{"order_id": req.OrderID, "sku": req.SKU}).Scan(&code)
			if err != nil && isUniqueViolation(err) {
				claimed = false
				err = a.pool.QueryRow(ctx, `
					SELECT code FROM inventory_keys WHERE order_id = @order_id
				`, pgx.NamedArgs{"order_id": req.OrderID}).Scan(&code)
			}
			if err != nil {
				return "", false, fiber.StatusConflict, nil
			}
			if claimed {
				if _, err := a.pool.Exec(ctx, `
					UPDATE products SET available_count = GREATEST(available_count - 1, 0) WHERE sku = @sku
				`, pgx.NamedArgs{"sku": req.SKU}); err != nil {
					return "", false, 0, err
				}
			}
			a.issued.Store(req.RequestID, code)
			shouldHang := toRate > 0 && rand.Float64() < toRate
			return code, shouldHang, fiber.StatusOK, nil
		}()
		if err != nil {
			return err
		}
		if status == fiber.StatusInternalServerError {
			return c.Status(status).JSON(issueResponse{Status: "error", Reason: "unavailable"})
		}
		if status == fiber.StatusConflict {
			return c.Status(status).JSON(issueResponse{Status: "error", Reason: "out_of_stock"})
		}
		if hang {
			time.Sleep(a.timeout + 1500*time.Millisecond)
		}
		return c.JSON(issueResponse{Status: "ok", RequestID: req.RequestID, Code: code})
	}
}
