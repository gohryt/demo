package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	testPool *pgxpool.Pool
	testApp  *app
	testBase string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	dsn := getenv("DATABASE_URL", "postgres://marketplace:marketplace@127.0.0.1:5432/marketplace?sslmode=disable")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db: %v\n", err)
		os.Exit(1)
	}
	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = pool.Ping(ctx)
		if pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		fmt.Fprintf(os.Stderr, "db ping: %v (start postgres: docker compose up -d)\n", pingErr)
		os.Exit(1)
	}
	if err := migrate(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}

	a := newApp(pool, config{providerTimeout: 300 * time.Millisecond})
	fiberApp := a.router()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	a.selfURL = "http://" + ln.Addr().String()
	wctx, cancel := context.WithCancel(ctx)
	go a.runWorker(wctx)
	go func() { _ = fiberApp.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}) }()
	time.Sleep(50 * time.Millisecond)

	testPool = pool
	testApp = a
	testBase = a.selfURL

	code := m.Run()
	cancel()
	_ = fiberApp.Shutdown()
	pool.Close()
	os.Exit(code)
}

func reset(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := execSimple(ctx, testPool, `
		UPDATE inventory_keys SET status = 'available', order_id = NULL, issued_at = NULL;
		DELETE FROM inventory_keys WHERE code LIKE 'OOS%';
		DELETE FROM ledger_entries;
		DELETE FROM payment_events;
		DELETE FROM orders;
		UPDATE products p SET available_count = (
		    SELECT count(*) FROM inventory_keys k WHERE k.sku = p.sku AND k.status = 'available'
		);
	`); err != nil {
		t.Fatal(err)
	}
	testApp.issued.Range(func(k, _ any) bool {
		testApp.issued.Delete(k)
		return true
	})
	testApp.errA, testApp.toA, testApp.errB, testApp.toB = 0, 0, 0, 0
}

func TestHappyPath(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "")
	if o.Status != "created" {
		t.Fatalf("status=%s", o.Status)
	}
	pay(t, "evt_"+o.ID, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "delivered")
	if got.Code == "" {
		t.Fatal("empty code")
	}
	if sumLedger(t, o.ID) != 0 {
		t.Fatalf("ledger=%d", sumLedger(t, o.ID))
	}
}

func TestRace50(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "")
	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		i := i
		go func() {
			defer wg.Done()
			pay(t, fmt.Sprintf("evt_%s_%d", o.ID, i), o.ID, "paid", o.Amount)
		}()
	}
	wg.Wait()
	got := waitStatus(t, o.ID, "delivered")
	if got.Code == "" {
		t.Fatal("empty code")
	}
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM inventory_keys WHERE order_id = @id
	`, pgx.NamedArgs{"id": o.ID}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("keys issued for order: %d", n)
	}
	if sumLedger(t, o.ID) != 0 {
		t.Fatalf("ledger=%d", sumLedger(t, o.ID))
	}
}

func TestSameEventID(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "")
	eid := "evt_same_" + o.ID
	pay(t, eid, o.ID, "paid", o.Amount)
	pay(t, eid, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "delivered")
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM payment_events WHERE order_id = @id
	`, pgx.NamedArgs{"id": o.ID}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("events=%d", n)
	}
	if got.Code == "" {
		t.Fatal("empty code")
	}
}

func TestWebhookBeforeOrder(t *testing.T) {
	reset(t)
	id := newID("ord_")
	pay(t, "evt_early_"+id, id, "paid", 500)
	o := createOrder(t, "STEAM-TOPUP-500", id)
	if o.Status != "paid" && o.Status != "delivering" && o.Status != "delivered" {
		t.Fatalf("after create status=%s", o.Status)
	}
	got := waitStatus(t, id, "delivered")
	if got.Code == "" {
		t.Fatal("empty code")
	}
}

func TestTimeoutSameCode(t *testing.T) {
	reset(t)
	testApp.toA = 1
	o := createOrder(t, "STEAM-TOPUP-500", "")
	pay(t, "evt_to_"+o.ID, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "delivered")
	if got.Code == "" {
		t.Fatal("empty code")
	}
	if got.Provider != "a" {
		t.Fatalf("provider=%s, want a (timeout must retry A, not fallback)", got.Provider)
	}
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM inventory_keys WHERE order_id = @id
	`, pgx.NamedArgs{"id": o.ID}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("keys=%d", n)
	}
}

func TestFallback(t *testing.T) {
	reset(t)
	testApp.errA = 1
	o := createOrder(t, "STEAM-TOPUP-500", "")
	pay(t, "evt_fb_"+o.ID, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "delivered")
	if got.Provider != "b" {
		t.Fatalf("provider=%s, want b", got.Provider)
	}
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM inventory_keys WHERE order_id = @id
	`, pgx.NamedArgs{"id": o.ID}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("keys=%d", n)
	}
}

func TestOutOfStockRecoverable(t *testing.T) {
	reset(t)
	o := createOrder(t, "GIFT-ROBLOX-800", "")
	pay(t, "evt_oos_"+o.ID, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "out_of_stock")
	if got.Status != "out_of_stock" {
		t.Fatalf("status=%s", got.Status)
	}
	ctx := context.Background()
	_, err := testPool.Exec(ctx, `
		INSERT INTO inventory_keys (sku, code, status)
		VALUES ('GIFT-ROBLOX-800', 'OOS1-TEST-KEY1', 'available')
	`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = testPool.Exec(ctx, `
		UPDATE products SET available_count = available_count + 1 WHERE sku = 'GIFT-ROBLOX-800'
	`)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, o.ID)
	got = waitStatus(t, o.ID, "delivered")
	if got.Code != "OOS1-TEST-KEY1" {
		t.Fatalf("code=%s", got.Code)
	}
}

func TestFailedThenPaid(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "")
	pay(t, "evt_fail_"+o.ID, o.ID, "failed", o.Amount)
	st := getOrderHTTP(t, o.ID)
	if st.Status != "payment_failed" {
		t.Fatalf("status=%s", st.Status)
	}
	pay(t, "evt_paid_"+o.ID, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "delivered")
	if got.Code == "" {
		t.Fatal("empty code")
	}
}

func TestCatalog(t *testing.T) {
	reset(t)
	resp := httpDo(t, http.MethodGet, testBase+"/api/v1/catalog?limit=20", nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var body struct {
		Products []productJSON `json:"products"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Products) == 0 {
		t.Fatal("empty catalog")
	}
}

func TestReconciliationHappyEmpty(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "")
	pay(t, "evt_rec_"+o.ID, o.ID, "paid", o.Amount)
	waitStatus(t, o.ID, "delivered")
	resp := httpDo(t, http.MethodGet, testBase+"/internal/reconciliation", nil)
	defer resp.Body.Close()
	var body struct {
		PaidNotDelivered []reconItem `json:"paid_not_delivered"`
		DeliveredNotPaid []reconItem `json:"delivered_not_paid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	for _, it := range body.PaidNotDelivered {
		if it.ID == o.ID {
			t.Fatalf("delivered order in paid_not_delivered")
		}
	}
	if len(body.DeliveredNotPaid) != 0 {
		t.Fatalf("delivered_not_paid=%v", body.DeliveredNotPaid)
	}
}

func TestSameEventIDNoDoubleIssue(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "")
	var wg sync.WaitGroup
	var okCount atomic.Int32
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			defer wg.Done()
			resp := httpDo(t, http.MethodPost, testBase+"/webhook/payment", paymentBody("evt_one_"+o.ID, o.ID, "paid", o.Amount))
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				okCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if okCount.Load() != 50 {
		t.Fatalf("accepted=%d", okCount.Load())
	}
	waitStatus(t, o.ID, "delivered")
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM inventory_keys WHERE order_id = @id
	`, pgx.NamedArgs{"id": o.ID}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("keys=%d", n)
	}
}

func createOrder(t *testing.T, sku, id string) orderJSON {
	t.Helper()
	body := fmt.Sprintf(`{"sku":%q,"id":%q}`, sku, id)
	resp := httpDo(t, http.MethodPost, testBase+"/api/v1/orders", []byte(body))
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create %d: %s", resp.StatusCode, raw)
	}
	var o orderJSON
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	return o
}

func pay(t *testing.T, eventID, orderID, status string, amount int) {
	t.Helper()
	resp := httpDo(t, http.MethodPost, testBase+"/webhook/payment", paymentBody(eventID, orderID, status, amount))
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("webhook %d: %s", resp.StatusCode, raw)
	}
}

func paymentBody(eventID, orderID, status string, amount int) []byte {
	return fmt.Appendf(nil, `{"event_id":%q,"order_id":%q,"status":%q,"amount":%d,"currency":"RUB","created_at":"2025-01-01T12:00:00Z"}`,
		eventID, orderID, status, amount)
}

func getOrderHTTP(t *testing.T, id string) orderJSON {
	t.Helper()
	resp := httpDo(t, http.MethodGet, testBase+"/api/v1/orders/"+id, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("get %d: %s", resp.StatusCode, raw)
	}
	var o orderJSON
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	return o
}

func waitStatus(t *testing.T, id, want string) orderJSON {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last orderJSON
	for time.Now().Before(deadline) {
		last = getOrderHTTP(t, id)
		if last.Status == want {
			return last
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("timeout waiting %s, last=%s err=%s", want, last.Status, last.Error)
	return last
}

func retry(t *testing.T, id string) {
	t.Helper()
	resp := httpDo(t, http.MethodPost, testBase+"/internal/orders/"+id+"/retry", nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("retry %d: %s", resp.StatusCode, raw)
	}
}

func sumLedger(t *testing.T, id string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), `
		SELECT coalesce(sum(amount), 0) FROM ledger_entries WHERE order_id = @id
	`, pgx.NamedArgs{"id": id}).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func httpDo(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
