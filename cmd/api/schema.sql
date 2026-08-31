CREATE TABLE IF NOT EXISTS products (
    sku              TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    type             TEXT NOT NULL,
    price            INT  NOT NULL,
    currency         TEXT NOT NULL DEFAULT 'RUB',
    available_count  INT  NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id               TEXT PRIMARY KEY,
    sku              TEXT NOT NULL REFERENCES products (sku),
    amount           INT  NOT NULL,
    currency         TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN (
        'created', 'paid', 'delivering', 'delivered',
        'payment_failed', 'out_of_stock', 'delivery_failed'
    )),
    issued_code      TEXT,
    issued_provider  TEXT,
    last_error       TEXT,
    fail_count       INT  NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    paid_at          TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS orders_worker_idx
    ON orders (updated_at)
    WHERE status IN ('paid', 'delivering', 'out_of_stock', 'delivery_failed');

CREATE TABLE IF NOT EXISTS inventory_keys (
    id         BIGSERIAL PRIMARY KEY,
    sku        TEXT NOT NULL REFERENCES products (sku),
    code       TEXT NOT NULL UNIQUE,
    status     TEXT NOT NULL CHECK (status IN ('available', 'issued')),
    order_id   TEXT UNIQUE REFERENCES orders (id),
    issued_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS inventory_keys_available_idx
    ON inventory_keys (sku)
    WHERE status = 'available';

CREATE TABLE IF NOT EXISTS payment_events (
    event_id     TEXT PRIMARY KEY,
    order_id     TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('paid', 'failed')),
    amount       INT  NOT NULL,
    currency     TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS payment_events_pending_idx
    ON payment_events (order_id)
    WHERE applied_at IS NULL;

CREATE TABLE IF NOT EXISTS ledger_entries (
    id          BIGSERIAL PRIMARY KEY,
    order_id    TEXT NOT NULL,
    kind        TEXT NOT NULL,
    amount      INT  NOT NULL,
    currency    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (order_id, kind)
);

CREATE INDEX IF NOT EXISTS products_vitrine_idx
    ON products (available_count DESC, sku)
    INCLUDE (name, type, price, currency);
