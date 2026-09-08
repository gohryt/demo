package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"
)

func createCart(t *testing.T, items ...itemRequest) orderJSON {
	t.Helper()
	body, _ := json.Marshal(createOrderReq{Items: items})
	resp := httpDo(t, "POST", testBase+"/api/v1/orders", body)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 201 {
		t.Fatalf("create cart: %d %s", resp.StatusCode, raw)
	}
	var o orderJSON
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	return o
}
func sqlCount(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func execTest(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}
func assertMoney(t *testing.T, o orderJSON, delivered, refunded int) {
	t.Helper()
	if o.Money.Paid != o.Amount || o.Money.Delivered != delivered || o.Money.Refunded != refunded || o.Money.Pending != 0 || sumLedger(t, o.ID) != 0 {
		t.Fatalf("money: %+v, amount=%d", o.Money, o.Amount)
	}
}

func TestPartialRefundAndReplay(t *testing.T) {
	reset(t)
	o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"}, itemRequest{"GIFT-ROBLOX-800", "b"})
	pay(t, "pay_"+o.ID, o.ID, "paid", o.Amount)
	got := waitStatus(t, o.ID, "partially_refunded")
	assertMoney(t, got, 500, 890)
	if got.Items[0].Code == nil || got.Items[1].Code != nil {
		t.Fatalf("items=%+v", got.Items)
	}
	for n := 0; n < 5; n++ {
		pay(t, fmt.Sprintf("replay_%d", n), o.ID, "paid", o.Amount)
		retry(t, o.ID)
	}
	assertMoney(t, getOrderHTTP(t, o.ID), 500, 890)
	if n := sqlCount(t, `SELECT count(*) FROM refund_receipts`); n != 1 {
		t.Fatalf("refunds=%d", n)
	}
	if n := sqlCount(t, `SELECT count(*) FROM ledger_entries`); n != 3 {
		t.Fatalf("entries=%d", n)
	}
}

func TestDishonestSupplier(t *testing.T) {
	for _, mode := range []string{"duplicate", "wrong_sku", "error_after_issue", "timeout_after_issue"} {
		t.Run(mode, func(t *testing.T) {
			reset(t)
			first := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
			pay(t, "first", first.ID, "paid", first.Amount)
			old := waitStatus(t, first.ID, "delivered")
			setMode(t, "a", mode, 10000)
			o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
			pay(t, "second", o.ID, "paid", o.Amount)
			got := waitStatus(t, o.ID, "delivered")
			if got.Code == "" || got.Code == old.Code {
				t.Fatalf("bad code %s", got.Code)
			}
			if n := sqlCount(t, `SELECT count(*) FROM inventory_keys WHERE code=$1 AND sku=$2 AND item_id=$3`, got.Code, got.Items[0].SKU, got.Items[0].ID); n != 1 {
				t.Fatal("code is not owned by this item")
			}
			if n := sqlCount(t, `SELECT count(*) FROM issuer_operations WHERE item_id=$1 AND outcome='issued'`, got.Items[0].ID); n != 1 {
				t.Fatalf("issues=%d", n)
			}
			if n := sqlCount(t, `SELECT count(*) FROM provider_discrepancies WHERE resolution='recovered_verified_code'`); n != 1 {
				t.Fatalf("resolved=%d", n)
			}
			assertMoney(t, got, 500, 0)
		})
	}
}

func TestRepeatedSKUAndConcurrentWorkers(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); testApp.runWorker(ctx) }()
	}
	defer func() { cancel(); wg.Wait() }()
	var orders []orderJSON
	for n := 0; n < 5; n++ {
		o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"}, itemRequest{"STEAM-TOPUP-500", "b"})
		orders = append(orders, o)
		pay(t, "pay_"+o.ID, o.ID, "paid", o.Amount)
	}
	codes := map[string]bool{}
	for _, o := range orders {
		got := waitStatus(t, o.ID, "delivered")
		assertMoney(t, got, 1000, 0)
		for _, i := range got.Items {
			if i.Code == nil || codes[*i.Code] {
				t.Fatal("missing or duplicate code")
			}
			codes[*i.Code] = true
		}
	}
	if n := sqlCount(t, `SELECT count(*) FROM inventory_keys WHERE status='issued'`); n != 10 {
		t.Fatalf("issues=%d", n)
	}
}

func TestRestartAfterIssuerCommit(t *testing.T) {
	reset(t)
	stopTestWorker()
	o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"}, itemRequest{"STEAM-TOPUP-1000", "b"})
	pay(t, "paid", o.ID, "paid", o.Amount)
	ctx := context.Background()
	if !testApp.processOne(ctx) {
		t.Fatal("first item not processed")
	}
	i, err := testApp.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	code, _, err := testApp.registryIssue(ctx, i.Provider, issueRequest{RequestID: requestID(i), ItemID: i.ID, OrderID: i.OrderID, SKU: i.SKU}, false)
	if err != nil || code == "" {
		t.Fatalf("issue %s %v", code, err)
	}
	// Lose the entire app object and its in-flight work. Only the database survives.
	execTest(t, `UPDATE order_items SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, i.ID)
	restarted := newApp(testPool, config{providerTimeout: 300 * time.Millisecond})
	restarted.selfURL = testBase
	if !restarted.processOne(ctx) {
		t.Fatal("restart did not recover work")
	}
	// A response from the old worker is fenced by its obsolete lease token.
	if err = testApp.finishIssue(ctx, i, issueResponse{Code: "BAD"}, false); err != nil {
		t.Fatal(err)
	}
	got := getOrderHTTP(t, o.ID)
	if got.Status != "delivered" {
		t.Fatalf("status=%s", got.Status)
	}
	assertMoney(t, got, 1500, 0)
	if *got.Items[1].Code != code {
		t.Fatal("recovery changed code")
	}
	if n := sqlCount(t, `SELECT count(*) FROM issuer_operations WHERE outcome='issued'`); n != 2 {
		t.Fatalf("issues=%d", n)
	}
}

func TestRestartAfterRefundCommit(t *testing.T) {
	reset(t)
	stopTestWorker()
	o := createCart(t, itemRequest{"GIFT-ROBLOX-800", "b"})
	pay(t, "paid", o.ID, "paid", o.Amount)
	execTest(t, `UPDATE order_items SET attempts=2 WHERE order_id=$1`, o.ID)
	if !testApp.processOne(context.Background()) {
		t.Fatal("issue was not attempted")
	}
	execTest(t, `UPDATE order_items SET next_attempt_at=clock_timestamp() WHERE order_id=$1`, o.ID)
	i, err := testApp.claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if i.State != "refund_pending" {
		t.Fatalf("state=%s", i.State)
	}
	if err = testApp.refund(context.Background(), i); err != nil {
		t.Fatal(err)
	}
	receiptAt := time.Now().UTC()
	receiptState := getOrderHTTP(t, o.ID)
	if receiptState.Money.RefundSent != 890 || receiptState.Money.RefundUnposted != 890 || receiptState.Money.Pending != 890 {
		t.Fatalf("external refund not visible: %+v", receiptState.Money)
	}
	execTest(t, `UPDATE order_items SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, i.ID)
	restarted := newApp(testPool, config{providerTimeout: 300 * time.Millisecond})
	restarted.selfURL = testBase
	if !restarted.processOne(context.Background()) {
		t.Fatal("refund not recovered")
	}
	got := getOrderHTTP(t, o.ID)
	if got.Status != "refunded" {
		t.Fatalf("status=%s", got.Status)
	}
	assertMoney(t, got, 0, 890)
	resp := httpDo(t, "GET", testBase+"/api/v1/orders/"+o.ID+"?at="+url.QueryEscape(receiptAt.Format(time.RFC3339Nano)), nil)
	defer resp.Body.Close()
	var historical orderJSON
	if err := json.NewDecoder(resp.Body).Decode(&historical); err != nil {
		t.Fatal(err)
	}
	if historical.Money.RefundUnposted != 890 || got.Money.RefundUnposted != 0 {
		t.Fatal("refund history was rewritten")
	}
	if n := sqlCount(t, `SELECT count(*) FROM refund_receipts`); n != 1 {
		t.Fatalf("refunds=%d", n)
	}
}

func TestCancellationFencesLateIssue(t *testing.T) {
	reset(t)
	stopTestWorker()
	o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
	pay(t, "paid", o.ID, "paid", o.Amount)
	ctx := context.Background()
	i, err := testApp.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = testApp.finishIssue(ctx, i, issueResponse{Reason: "connection lost before issue"}, false); err != nil {
		t.Fatal(err)
	}
	code, _, err := testApp.registryIssue(ctx, i.Provider, issueRequest{RequestID: requestID(i), ItemID: i.ID, OrderID: i.OrderID, SKU: i.SKU}, false)
	if err != nil || code != "" {
		t.Fatalf("late issue: %s %v", code, err)
	}
	retry(t, o.ID)
	if !testApp.processOne(ctx) {
		t.Fatal("retry not processed")
	}
	assertMoney(t, getOrderHTTP(t, o.ID), 500, 0)
	if n := sqlCount(t, `SELECT count(*) FROM inventory_keys WHERE status='issued'`); n != 1 {
		t.Fatalf("keys=%d", n)
	}
}

func TestRateLimitQueueAndPaidPriority(t *testing.T) {
	reset(t)
	stopTestWorker()
	setMode(t, "a", "normal", 2)
	unpaid := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
	var orders []orderJSON
	for n := 0; n < 5; n++ {
		o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
		orders = append(orders, o)
		pay(t, fmt.Sprintf("paid_%d", n), o.ID, "paid", o.Amount)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	for n := 0; n < 5; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); testApp.processOne(ctx) }()
	}
	wg.Wait()
	if n := sqlCount(t, `SELECT count(*) FROM provider_calls WHERE provider='a'`); n != 2 {
		t.Fatalf("admitted=%d", n)
	}
	if n := sqlCount(t, `SELECT count(*) FROM order_items WHERE state='delivered'`); n != 2 {
		t.Fatalf("delivered=%d", n)
	}
	if got := getOrderHTTP(t, unpaid.ID); got.Status != "created" {
		t.Fatal("unpaid order consumed quota")
	}
	if testApp.processOne(ctx) {
		t.Fatal("limited queue must wait")
	}
	response := httpDo(t, "GET", testBase+"/internal/queue", nil)
	defer response.Body.Close()
	var progress struct {
		Providers []struct {
			Provider  string `json:"provider"`
			Queued    int    `json:"queued_orders"`
			Delivered int    `json:"delivered_items"`
		} `json:"providers"`
	}
	if err := json.NewDecoder(response.Body).Decode(&progress); err != nil {
		t.Fatal(err)
	}
	if progress.Providers[0].Queued != 3 || progress.Providers[0].Delivered != 2 {
		t.Fatalf("progress=%+v", progress)
	}
	// Move only the test clock fixture. Production never edits admission timestamps.
	execTest(t, `UPDATE provider_calls SET admitted_at=clock_timestamp()-interval '61 seconds'`)
	execTest(t, `UPDATE order_items SET next_attempt_at=clock_timestamp() WHERE lease_token IS NULL`)
	for n := 0; n < 2; n++ {
		if !testApp.processOne(ctx) {
			t.Fatal("queue did not resume after window")
		}
	}
	execTest(t, `UPDATE provider_calls SET admitted_at=clock_timestamp()-interval '61 seconds'`)
	if !testApp.processOne(ctx) {
		t.Fatal("last order lost")
	}
	for _, o := range orders {
		assertMoney(t, getOrderHTTP(t, o.ID), 500, 0)
	}
}

func TestHistoryAndPeriodReport(t *testing.T) {
	reset(t)
	stopTestWorker()
	from := time.Now().UTC()
	o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
	createdAt := time.Now().UTC()
	pay(t, "paid", o.ID, "paid", o.Amount)
	paidAt := time.Now().UTC()
	if !testApp.processOne(context.Background()) {
		t.Fatal("no work")
	}
	for _, tc := range []struct {
		at     time.Time
		status string
		paid   int
	}{{createdAt, "created", 0}, {paidAt, "paid", 500}} {
		resp := httpDo(t, "GET", testBase+"/api/v1/orders/"+o.ID+"?at="+url.QueryEscape(tc.at.Format(time.RFC3339Nano)), nil)
		var got orderJSON
		err := json.NewDecoder(resp.Body).Decode(&got)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != tc.status || got.Money.Paid != tc.paid {
			t.Fatalf("history=%+v", got)
		}
	}
	resp := httpDo(t, "GET", testBase+"/internal/reports?from="+url.QueryEscape(from.Format(time.RFC3339Nano))+"&to="+url.QueryEscape(time.Now().UTC().Format(time.RFC3339Nano)), nil)
	defer resp.Body.Close()
	var report struct {
		Currencies []struct {
			Paid, Delivered, Refunded int
			Opening                   int `json:"opening_pending"`
			Closing                   int `json:"closing_pending"`
		} `json:"currencies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	r := report.Currencies[0]
	if r.Paid != 500 || r.Delivered != 500 || r.Opening+r.Paid != r.Delivered+r.Refunded+r.Closing {
		t.Fatalf("report=%+v", r)
	}
	for _, q := range []string{`UPDATE order_history SET kind='rewrite'`, `DELETE FROM ledger_entries`, `TRUNCATE order_history`, `UPDATE issuer_operations SET reason='rewrite'`} {
		if _, err := testPool.Exec(context.Background(), q); err == nil {
			t.Fatalf("mutation allowed: %s", q)
		}
	}
}

func TestPaymentValidationAndIdempotencyPayload(t *testing.T) {
	reset(t)
	o := createOrder(t, "STEAM-TOPUP-500", "known_order")
	resp := httpDo(t, "POST", testBase+"/webhook/payment", paymentBody("bad", o.ID, "paid", 1))
	resp.Body.Close()
	if resp.StatusCode != 422 {
		t.Fatalf("amount accepted: %d", resp.StatusCode)
	}
	pay(t, "good", o.ID, "paid", 500)
	waitStatus(t, o.ID, "delivered")
	resp = httpDo(t, "POST", testBase+"/webhook/payment", paymentBody("good", o.ID, "paid", 1))
	resp.Body.Close()
	if resp.StatusCode != 409 {
		t.Fatalf("event conflict: %d", resp.StatusCode)
	}
	for _, tc := range []struct {
		sku    string
		status int
	}{{"STEAM-TOPUP-500", 200}, {"STEAM-TOPUP-1000", 409}} {
		resp = httpDo(t, "POST", testBase+"/api/v1/orders", []byte(fmt.Sprintf(`{"id":"known_order","sku":%q}`, tc.sku)))
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Fatalf("cart retry: %d", resp.StatusCode)
		}
	}
	pay(t, "early_bad", "early_order", "paid", 1)
	early := createOrder(t, "STEAM-TOPUP-500", "early_order")
	if early.Status != "created" || early.Money.Paid != 0 {
		t.Fatalf("early invalid payment applied: %+v", early)
	}
	// Same event id cannot be moved to another order.
	resp = httpDo(t, http.MethodPost, testBase+"/webhook/payment", paymentBody("good", early.ID, "paid", 500))
	resp.Body.Close()
	if resp.StatusCode != 409 {
		t.Fatalf("cross-order event accepted: %d", resp.StatusCode)
	}
}
