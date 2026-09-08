package main

import (
	"context"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
	"time"
)

func recordHistory(ctx context.Context, tx pgx.Tx, id, kind string) error {
	_, err := tx.Exec(ctx, `INSERT INTO order_history(order_id,kind,snapshot) VALUES($1,$2,order_snapshot($1))`, id, kind)
	return err
}

// Period deltas are derived exclusively from the immutable history. Half-open
// intervals [from,to) use balances just before each boundary.
func (a *app) periodReport(c fiber.Ctx) error {
	from, err := time.Parse(time.RFC3339Nano, c.Query("from"))
	if err != nil {
		return fiber.NewError(400, "invalid from")
	}
	to, err := time.Parse(time.RFC3339Nano, c.Query("to"))
	if err != nil || !from.Before(to) {
		return fiber.NewError(400, "invalid to")
	}
	rows, err := a.pool.Query(c.Context(), `WITH boundaries AS (
  SELECT o.id,o.currency,
    coalesce((SELECT snapshot->'money' FROM order_history WHERE order_id=o.id AND recorded_at<$1 ORDER BY recorded_at DESC,id DESC LIMIT 1),'{}'::jsonb) AS opening,
    coalesce((SELECT snapshot->'money' FROM order_history WHERE order_id=o.id AND recorded_at<$2 ORDER BY recorded_at DESC,id DESC LIMIT 1),'{}'::jsonb) AS closing
  FROM orders o)
 SELECT currency,
  sum(coalesce((opening->>'pending')::bigint,0))::bigint AS opening_pending,
  sum(coalesce((closing->>'paid')::bigint,0)-coalesce((opening->>'paid')::bigint,0))::bigint AS paid,
  sum(coalesce((closing->>'delivered')::bigint,0)-coalesce((opening->>'delivered')::bigint,0))::bigint AS delivered,
  sum(coalesce((closing->>'refunded')::bigint,0)-coalesce((opening->>'refunded')::bigint,0))::bigint AS refunded,
  sum(coalesce((closing->>'pending')::bigint,0))::bigint AS closing_pending
 FROM boundaries GROUP BY currency ORDER BY currency`, from, to)
	if err != nil {
		return err
	}
	type report struct {
		Currency  string `json:"currency"`
		Opening   int64  `json:"opening_pending" db:"opening_pending"`
		Paid      int64  `json:"paid"`
		Delivered int64  `json:"delivered"`
		Refunded  int64  `json:"refunded"`
		Closing   int64  `json:"closing_pending" db:"closing_pending"`
	}
	result, err := pgx.CollectRows(rows, pgx.RowToStructByName[report])
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{"from": from, "to": to, "currencies": result})
}

func (a *app) queueProgress(c fiber.Ctx) error {
	rows, err := a.pool.Query(c.Context(), `SELECT p.provider,
  count(*) FILTER(WHERE o.paid_at IS NOT NULL AND i.state IN ('pending','issuing','refund_pending')) AS queued_items,
  count(DISTINCT o.id) FILTER(WHERE o.paid_at IS NOT NULL AND i.state IN ('pending','issuing','refund_pending')) AS queued_orders,
  count(i.id) FILTER(WHERE o.paid_at IS NULL) AS unpaid_items,
  count(*) FILTER(WHERE i.state='delivered') AS delivered_items,
  count(*) FILTER(WHERE i.state='refunded') AS refunded_items,
  p.requests_per_minute,
  (SELECT count(*) FROM provider_calls pc WHERE pc.provider=p.provider AND pc.admitted_at>clock_timestamp()-interval '1 minute') AS requests_last_minute
  FROM provider_settings p LEFT JOIN order_items i ON i.provider=p.provider LEFT JOIN orders o ON o.id=i.order_id
  GROUP BY p.provider ORDER BY p.provider`)
	if err != nil {
		return err
	}
	result, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{"providers": result})
}
