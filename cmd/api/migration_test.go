package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrationFromStageOne(t *testing.T) {
	ctx := context.Background()
	schema := "migration_" + newID("")
	execTest(t, `CREATE SCHEMA `+schema)
	defer execTest(t, `DROP SCHEMA `+schema+` CASCADE`)
	cfg := testPool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, name := range []string{"schema.sql", "seed.sql", "seed_load.sql"} {
		body, _ := sqlFiles.ReadFile(name)
		if err = execSimple(ctx, pool, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err = execSimple(ctx, pool, `
 INSERT INTO orders(id,sku,amount,currency,status,paid_at,issued_code,issued_provider)
 VALUES('old_done','STEAM-TOPUP-500',500,'RUB','delivered',now(),'LFXC-TNCS-BPCD','a'),
       ('old_inflight','STEAM-TOPUP-500',500,'RUB','delivering',now(),NULL,NULL);
 UPDATE inventory_keys SET status='issued',order_id='old_done' WHERE code='LFXC-TNCS-BPCD';
 UPDATE inventory_keys SET status='issued',order_id='old_inflight' WHERE code='P3EI-W8UO-9B4K';
 INSERT INTO ledger_entries(order_id,kind,amount,currency)
 VALUES('old_done','payment_credit',500,'RUB'),('old_done','delivery_debit',-500,'RUB'),('old_inflight','payment_credit',500,'RUB');
 `); err != nil {
		t.Fatal(err)
	}
	if err = migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err = migrate(ctx, pool); err != nil {
		t.Fatalf("migration replay: %v", err)
	}
	var raw []byte
	if err = pool.QueryRow(ctx, `SELECT order_snapshot('old_done')`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var o orderJSON
	if err = json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	if o.Code != "LFXC-TNCS-BPCD" || len(o.Items) != 1 || o.Money.Pending != 0 {
		t.Fatalf("old delivery lost: %+v", o)
	}
	a := newApp(pool, config{providerTimeout: 300 * time.Millisecond})
	i, err := a.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	code, _, err := a.registryIssue(ctx, i.Provider, issueRequest{RequestID: requestID(i), ItemID: i.ID, OrderID: i.OrderID, SKU: i.SKU}, false)
	if err != nil {
		t.Fatal(err)
	}
	if code != "P3EI-W8UO-9B4K" {
		t.Fatalf("lost existing reservation: %s", code)
	}
	if err = a.finishIssue(ctx, i, issueResponse{RequestID: requestID(i), Code: code}, false); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory_keys WHERE status='issued'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("duplicate issue after migration: %d", count)
	}
}

func TestPermitExpiryAndReplay(t *testing.T) {
	reset(t)
	stopTestWorker()
	setMode(t, "a", "normal", 1)
	ctx := context.Background()
	permit, err := testApp.reserveProvider(ctx, "a", "request1")
	if err != nil || permit == 0 {
		t.Fatalf("permit: %d %v", permit, err)
	}
	if next, err := testApp.reserveProvider(ctx, "a", "request2"); err != nil || next != 0 {
		t.Fatalf("reserved beyond limit: %d %v", next, err)
	}
	execTest(t, `UPDATE provider_calls SET valid_until=clock_timestamp()-interval '1 second' WHERE id=$1`, permit)
	req := issueRequest{RequestID: "request1", PermitID: permit}
	if _, ok, err := testApp.consumePermit(ctx, "a", req); err != nil || ok {
		t.Fatalf("expired permit accepted: %t %v", ok, err)
	}
	permit, err = testApp.reserveProvider(ctx, "a", "request2")
	if err != nil || permit == 0 {
		t.Fatalf("quota not recovered: %d %v", permit, err)
	}
	req = issueRequest{RequestID: "request2", PermitID: permit}
	if _, ok, err := testApp.consumePermit(ctx, "a", req); err != nil || !ok {
		t.Fatalf("valid permit rejected: %t %v", ok, err)
	}
	if _, ok, err := testApp.consumePermit(ctx, "a", req); err != nil || ok {
		t.Fatalf("permit replay accepted: %t %v", ok, err)
	}
	if n := sqlCount(t, `SELECT count(*) FROM provider_calls WHERE admitted_at IS NOT NULL`); n != 1 {
		t.Fatalf("admitted=%d", n)
	}
}

func TestDatabaseRejectsUnbalancedTerminalState(t *testing.T) {
	reset(t)
	stopTestWorker()
	o := createCart(t, itemRequest{"STEAM-TOPUP-500", "a"})
	pay(t, "paid", o.ID, "paid", o.Amount)
	if _, err := testPool.Exec(context.Background(), `UPDATE orders SET status='delivered' WHERE id=$1`, o.ID); err == nil {
		t.Fatal("unbalanced terminal order accepted")
	}
	if _, err := testPool.Exec(context.Background(), `INSERT INTO ledger_entries(order_id,item_id,kind,amount,currency) VALUES($1,$2,'refund_debit',-1000,'RUB')`, o.ID, o.Items[0].ID); err == nil {
		t.Fatal("over-refund accepted")
	}
	if got := getOrderHTTP(t, o.ID); got.Status != "paid" || got.Money.Pending != 500 {
		t.Fatalf("failed transaction changed order: %+v", got)
	}
}
