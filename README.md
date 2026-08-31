# Marketplace — ядро магазина цифровых товаров

Тестовое задание: заказ → вебхук оплаты → ровно одна выдача ключа. Go + Fiber v3 + pgx + PostgreSQL 17.

## Запуск

```bash
docker compose up -d
go run ./cmd/api
```

Переменные:

| Переменная | По умолчанию | Смысл |
|------------|--------------|--------|
| `DATABASE_URL` | `postgres://marketplace:marketplace@127.0.0.1:5432/marketplace?sslmode=disable` | Postgres |
| `HTTP_ADDR` | `:8080` | listen |
| `PROVIDER_TIMEOUT` | `2s` | клиентский таймаут к поставщику |
| `PROVIDER_A_ERROR_RATE` | `0` | доля 5xx у заглушки A (0…1) |
| `PROVIDER_A_TIMEOUT_RATE` | `0` | доля зависаний A после выдачи кода |
| `PROVIDER_B_ERROR_RATE` | `0` | то же для B |
| `PROVIDER_B_TIMEOUT_RATE` | `0` | то же для B |

API:

```
POST /api/v1/orders              {"sku":"STEAM-TOPUP-500","id":"ord_...?"}
GET  /api/v1/orders/:id
GET  /api/v1/catalog?limit=50
POST /webhook/payment            контракт из задания
GET  /internal/reconciliation
POST /internal/orders/:id/retry
GET  /health
```

## Тесты

Нужен поднятый Postgres (`docker compose up -d`).

```bash
go test ./cmd/api -count=1 -timeout 120s
```

Критерии приёмки:

| Сценарий | Тест / скрипт |
|----------|----------------|
| 50 параллельных paid → одна выдача | `TestRace50`, `scripts/race.sh` |
| тот же `event_id` | `TestSameEventID`, `TestSameEventIDNoDoubleIssue` |
| вебхук раньше заказа | `TestWebhookBeforeOrder` |
| таймаут, код уже выдан, повтор без второй выдачи | `TestTimeoutSameCode` |
| A мёртв → fallback B, один ключ | `TestFallback` |
| пустой остаток, восстановление | `TestOutOfStockRecoverable` |

Гонка против живого сервера:

```bash
docker compose up -d
go run ./cmd/api
./scripts/race.sh
```

Fallback:

```bash
PROVIDER_A_ERROR_RATE=1 PROVIDER_B_ERROR_RATE=0 go run ./cmd/api
# в другом терминале: ./scripts/race.sh
# в заказе provider = b
```

Ловушка таймаута:

```bash
PROVIDER_A_TIMEOUT_RATE=1 PROVIDER_TIMEOUT=300ms go run ./cmd/api
# ./scripts/race.sh
# provider = a, один code; повтор с тем же request_id не берёт второй ключ
```

## Ключевые решения

1. **Вебхук только пишет событие и CAS статуса, выдача в воркере.** Иначе 50 параллельных paid = 50 походов к поставщику.
2. **`request_id = req_{order_id}_{a\|b}` не меняется на ретраях.** Заглушка кладёт код в `sync.Map` *до* возможного зависания. Повтор возвращает тот же код.
3. **Timeout ≠ ошибка.** На B переключаемся только после явного 4xx/5xx без кода.
4. **Очередь — сам заказ.** Воркер: `SELECT … FOR UPDATE SKIP LOCKED` по `paid/delivering/out_of_stock/delivery_failed`. Отдельной таблицы jobs нет.
5. **`event_id` unique + advisory lock на `order_id`.** Повтор вебхука и гонка «вебхук vs create» не двоят оплату.
6. **`paid` побеждает `failed`**, если события пришли не по порядку.
7. **pgx напрямую**, без sqlc/ORM/интерфейсов. SQL рядом со сценарием.
8. **`available_count` денормализован** и меняется в той же выдаче ключа. Витрина не считает `inventory_keys`.

## Витрина под нагрузкой

Запрос:

```sql
SELECT sku, name, type, price, currency, available_count
  FROM products
 ORDER BY available_count DESC, sku
 LIMIT $1 OFFSET $2;
```

Индекс `products_vitrine_idx (available_count DESC, sku) INCLUDE (name, type, price, currency)` — index-only scan. В БД ~5000 SKU из `seed_load.sql`. После запуска:

```bash
psql "$DATABASE_URL" -c "EXPLAIN (ANALYZE, BUFFERS)
SELECT sku, name, type, price, currency, available_count
FROM products ORDER BY available_count DESC, sku LIMIT 50;"
```

Фактический план:

```
Limit  (cost=0.41..24.58 rows=50 width=39) (actual time=0.050..0.177 rows=50 loops=1)
  ->  Index Only Scan using products_vitrine_idx on products
        (cost=0.41..2423.59 rows=5012 width=39)
        (actual time=0.048..0.171 rows=50 loops=1)
        Heap Fetches: 63
        Buffers: shared hit=31
Execution Time: 0.196 ms
```

## Как масштабировали бы

Воркер уже выбирает заказы из Postgres — его можно вынести в отдельный процесс без смены схемы. Несколько воркеров безопасны: `SKIP LOCKED` + стабильный `request_id` + unique `inventory_keys.order_id`. Дальше: PgBouncer (transaction), партиции/архив старых `delivered`, витрина с read-replica или коротким кэшем по `available_count`. Идемпотентность остаётся на unique-ограничениях, не на кэше.

## Время

По факту: около 3 часов (план + реализация + прогон).
