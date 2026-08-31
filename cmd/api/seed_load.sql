INSERT INTO products (sku, name, type, price, currency, available_count)
SELECT
    'LOAD-' || lpad(i::text, 4, '0'),
    'Load SKU ' || i,
    'key',
    100 + (i % 50),
    'RUB',
    i % 20
FROM generate_series(1, 5000) AS s(i)
ON CONFLICT (sku) DO NOTHING;
