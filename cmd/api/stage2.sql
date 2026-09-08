ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check CHECK (status IN (
    'created', 'paid', 'delivering', 'delivered', 'payment_failed',
    'out_of_stock', 'delivery_failed', 'partially_refunded', 'refunded'
));
ALTER TABLE payment_events ADD COLUMN rejection_reason TEXT;

CREATE TABLE order_items (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(id),
    position INT NOT NULL,
    sku TEXT NOT NULL REFERENCES products(sku),
    provider TEXT NOT NULL CHECK (provider IN ('a', 'b')),
    fallback BOOLEAN NOT NULL DEFAULT false,
    amount INT NOT NULL CHECK (amount > 0),
    state TEXT NOT NULL CHECK (state IN ('pending','issuing','delivered','refund_pending','refunded')),
    code TEXT UNIQUE,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    lease_token TEXT,
    last_error TEXT,
    UNIQUE(order_id, position),
    CHECK ((state = 'delivered') = (code IS NOT NULL))
);
CREATE INDEX order_items_queue_idx ON order_items(next_attempt_at) WHERE state IN ('pending','issuing','refund_pending');
INSERT INTO order_items(id,order_id,position,sku,provider,fallback,amount,state,code)
SELECT 'item_' || id || '_1', id, 1, sku, coalesce(issued_provider,'a'), true, amount,
       CASE WHEN status='delivered' THEN 'delivered' ELSE 'pending' END, issued_code FROM orders;

ALTER TABLE inventory_keys DROP CONSTRAINT inventory_keys_order_id_key;
ALTER TABLE inventory_keys ADD COLUMN item_id TEXT UNIQUE REFERENCES order_items(id);
UPDATE inventory_keys k SET item_id = i.id FROM order_items i WHERE k.order_id=i.order_id;

ALTER TABLE ledger_entries DROP CONSTRAINT ledger_entries_order_id_kind_key;
ALTER TABLE ledger_entries ADD COLUMN item_id TEXT REFERENCES order_items(id);
UPDATE ledger_entries l SET item_id=i.id FROM order_items i WHERE l.order_id=i.order_id AND kind='delivery_debit';
CREATE UNIQUE INDEX ledger_operation_unique ON ledger_entries(order_id,kind,coalesce(item_id,''));
CREATE UNIQUE INDEX ledger_item_settlement_unique ON ledger_entries(item_id) WHERE kind IN ('delivery_debit','refund_debit');
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_sign CHECK (
    (kind='payment_credit' AND amount>0 AND item_id IS NULL) OR
    (kind IN ('delivery_debit','refund_debit') AND amount<0 AND item_id IS NOT NULL)
);

-- Independent issuer registry: terminal results also fence delayed issue calls.
CREATE TABLE issuer_operations (
    request_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL REFERENCES order_items(id),
    provider TEXT NOT NULL,
    code TEXT UNIQUE REFERENCES inventory_keys(code),
    outcome TEXT NOT NULL CHECK(outcome IN ('issued','rejected')),
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((outcome='issued') = (code IS NOT NULL))
);
CREATE UNIQUE INDEX issuer_one_issue_per_item ON issuer_operations(item_id) WHERE outcome='issued';
CREATE TABLE refund_receipts (
    request_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL UNIQUE REFERENCES order_items(id),
    amount INT NOT NULL CHECK(amount>0),
    currency TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE provider_settings (
    provider TEXT PRIMARY KEY CHECK(provider IN ('a','b')),
    mode TEXT NOT NULL DEFAULT 'normal' CHECK(mode IN ('normal','unavailable','duplicate','wrong_sku','error_after_issue','timeout_after_issue')),
    requests_per_minute INT NOT NULL DEFAULT 60 CHECK(requests_per_minute>0)
);
INSERT INTO provider_settings(provider) VALUES ('a'),('b');
CREATE TABLE provider_calls (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL REFERENCES provider_settings(provider),
    request_id TEXT NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    valid_until TIMESTAMPTZ NOT NULL,
    admitted_at TIMESTAMPTZ
);
CREATE INDEX provider_calls_window_idx ON provider_calls(provider,admitted_at);
CREATE TABLE provider_discrepancies (
    request_id TEXT PRIMARY KEY REFERENCES issuer_operations(request_id),
    reported_code TEXT NOT NULL,
    canonical_code TEXT NOT NULL,
    reason TEXT NOT NULL,
    resolution TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE order_history (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(id),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    kind TEXT NOT NULL,
    snapshot JSONB NOT NULL
);
CREATE INDEX order_history_at_idx ON order_history(order_id,recorded_at DESC,id DESC);
CREATE FUNCTION order_snapshot(order_key TEXT) RETURNS JSONB LANGUAGE sql STABLE AS $$
SELECT jsonb_build_object('id',o.id,'sku',o.sku,'amount',o.amount,'currency',o.currency,
    'status',o.status,'code',coalesce(o.issued_code,''),'provider',coalesce(o.issued_provider,''),
    'error',coalesce(o.last_error,''),
    'items',coalesce((SELECT jsonb_agg(jsonb_build_object('id',i.id,'sku',i.sku,'provider',i.provider,
         'amount',i.amount,'state',i.state,'code',i.code,'attempts',i.attempts,'error',i.last_error)
         ORDER BY i.position) FROM order_items i WHERE i.order_id=o.id),'[]'::jsonb),
    'money',jsonb_build_object(
         'paid',coalesce((SELECT sum(amount) FROM ledger_entries WHERE order_id=o.id AND kind='payment_credit'),0),
         'delivered',-coalesce((SELECT sum(amount) FROM ledger_entries WHERE order_id=o.id AND kind='delivery_debit'),0),
         'refunded',-coalesce((SELECT sum(amount) FROM ledger_entries WHERE order_id=o.id AND kind='refund_debit'),0),
         'pending',coalesce((SELECT sum(amount) FROM ledger_entries WHERE order_id=o.id),0),
         'refund_sent',coalesce((SELECT sum(r.amount) FROM refund_receipts r JOIN order_items i ON i.id=r.item_id WHERE i.order_id=o.id),0),
         'refund_unposted',coalesce((SELECT sum(r.amount) FROM refund_receipts r JOIN order_items i ON i.id=r.item_id WHERE i.order_id=o.id),0)
              +coalesce((SELECT sum(amount) FROM ledger_entries WHERE order_id=o.id AND kind='refund_debit'),0)))
FROM orders o WHERE o.id=order_key;
$$;
CREATE FUNCTION immutable_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'append-only table: %', TG_TABLE_NAME; END $$;
CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON ledger_entries FOR EACH STATEMENT EXECUTE FUNCTION immutable_history();
CREATE TRIGGER history_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON order_history FOR EACH STATEMENT EXECUTE FUNCTION immutable_history();
CREATE TRIGGER receipts_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON refund_receipts FOR EACH STATEMENT EXECUTE FUNCTION immutable_history();
CREATE TRIGGER issuer_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON issuer_operations FOR EACH STATEMENT EXECUTE FUNCTION immutable_history();
CREATE TRIGGER discrepancies_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON provider_discrepancies FOR EACH STATEMENT EXECUTE FUNCTION immutable_history();

-- Deferred checks inspect the final transaction state, after all item and ledger
-- writes, and reject a terminal order unless every monetary component agrees.
CREATE FUNCTION check_order_balance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    order_key TEXT;
    o orders%ROWTYPE;
    paid BIGINT; delivered BIGINT; refunded BIGINT;
    item_total BIGINT; item_delivered BIGINT; item_refunded BIGINT; unfinished BIGINT;
    sent BIGINT;
BEGIN
    IF TG_TABLE_NAME='orders' THEN order_key:=NEW.id; ELSE order_key:=NEW.order_id; END IF;
    SELECT * INTO o FROM orders WHERE id=order_key;
    SELECT coalesce(sum(amount) FILTER(WHERE kind='payment_credit'),0),
           -coalesce(sum(amount) FILTER(WHERE kind='delivery_debit'),0),
           -coalesce(sum(amount) FILTER(WHERE kind='refund_debit'),0)
      INTO paid,delivered,refunded FROM ledger_entries WHERE order_id=order_key;
    IF paid<delivered+refunded OR (paid<>0 AND paid<>o.amount) THEN
        RAISE EXCEPTION 'money invariant violated for %',order_key;
    END IF;
    IF EXISTS(SELECT 1 FROM ledger_entries l LEFT JOIN order_items i ON i.id=l.item_id
        WHERE l.order_id=order_key AND (l.currency<>o.currency OR
            (l.item_id IS NOT NULL AND (i.order_id<>order_key OR -l.amount<>i.amount)))) THEN
        RAISE EXCEPTION 'ledger payload mismatch for %',order_key;
    END IF;
    IF o.status IN ('delivered','partially_refunded','refunded') THEN
        SELECT coalesce(sum(amount),0),coalesce(sum(amount) FILTER(WHERE state='delivered'),0),
               coalesce(sum(amount) FILTER(WHERE state='refunded'),0),
               count(*) FILTER(WHERE state NOT IN ('delivered','refunded'))
          INTO item_total,item_delivered,item_refunded,unfinished FROM order_items WHERE order_id=order_key;
        SELECT coalesce(sum(r.amount),0) INTO sent FROM refund_receipts r JOIN order_items i ON i.id=r.item_id WHERE i.order_id=order_key;
        IF paid<>o.amount OR paid<>delivered+refunded OR item_total<>o.amount OR unfinished<>0
            OR delivered<>item_delivered OR refunded<>item_refunded OR refunded<>sent
            OR (o.status='delivered' AND refunded<>0) OR (o.status='refunded' AND delivered<>0)
            OR (o.status='partially_refunded' AND (delivered=0 OR refunded=0)) THEN
            RAISE EXCEPTION 'terminal money invariant violated for %',order_key;
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER orders_balance AFTER INSERT OR UPDATE ON orders DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_order_balance();
CREATE CONSTRAINT TRIGGER ledger_balance AFTER INSERT ON ledger_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_order_balance();
CREATE CONSTRAINT TRIGGER items_balance AFTER INSERT OR UPDATE ON order_items DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_order_balance();
