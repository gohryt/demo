-- Expected result: zero rows, including while orders are still in progress.
WITH money AS (
    SELECT o.id, o.status, o.amount, o.currency, o.paid_at,
           coalesce(sum(l.amount) FILTER (WHERE l.kind='payment_credit'),0) AS paid,
           -coalesce(sum(l.amount) FILTER (WHERE l.kind='delivery_debit'),0) AS delivered,
           -coalesce(sum(l.amount) FILTER (WHERE l.kind='refund_debit'),0) AS refunded
    FROM orders o LEFT JOIN ledger_entries l ON l.order_id=o.id
    GROUP BY o.id
), items AS (
    SELECT order_id, sum(amount) AS amount,
           coalesce(sum(amount) FILTER (WHERE state='delivered'),0) AS delivered,
           coalesce(sum(amount) FILTER (WHERE state='refunded'),0) AS refunded
    FROM order_items GROUP BY order_id
)
SELECT m.*, m.paid-m.delivered-m.refunded AS pending
FROM money m JOIN items i ON i.order_id=m.id
WHERE m.amount<>i.amount
   OR m.paid<>CASE WHEN m.paid_at IS NULL THEN 0 ELSE m.amount END
   OR m.delivered<>i.delivered OR m.refunded<>i.refunded
   OR m.paid<m.delivered+m.refunded
   OR (m.status IN ('delivered','partially_refunded','refunded')
       AND m.paid<>m.delivered+m.refunded);
