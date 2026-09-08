#!/usr/bin/env python3
"""Deterministic stage 2 demos against a running API; Python standard library only."""
import argparse
import json
import os
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8080")


def api(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def mode(provider, value, limit=60):
    api("PUT", f"/stub/providers/{provider}",
        {"mode": value, "requests_per_minute": limit})


def create(items):
    order = api("POST", "/api/v1/orders", {"id": "demo_" + uuid.uuid4().hex, "items": items})
    print("order", order["id"], flush=True)
    return order


def pay(order):
    payload = {"event_id": "evt_" + order["id"], "order_id": order["id"],
               "status": "paid", "amount": order["amount"], "currency": order["currency"]}
    for _ in range(3):
        api("POST", "/webhook/payment", payload)


def wait(order, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = api("GET", "/api/v1/orders/" + order["id"])
        if current["status"] in ("delivered", "partially_refunded", "refunded"):
            money = current["money"]
            assert money["paid"] == money["delivered"] + money["refunded"]
            assert money["pending"] == 0
            return current
        time.sleep(.2)
    raise RuntimeError(f"order did not finish: {current}")


def partial():
    mode("a", "normal")
    mode("b", "normal")
    order = create([{"sku": "STEAM-TOPUP-500", "provider": "a"},
                    {"sku": "GIFT-ROBLOX-800", "provider": "b"}])
    pay(order)
    result = wait(order)
    assert result["status"] == "partially_refunded", "Use a database with available Steam keys and no Roblox stock"
    assert result["money"]["delivered"] == 500
    assert result["money"]["refunded"] == 890
    api("POST", "/internal/orders/" + order["id"] + "/retry")
    assert wait(order)["money"] == result["money"]
    print(json.dumps(result, ensure_ascii=False, indent=2))


def dishonest():
    mode("a", "normal")
    first = create([{"sku": "STEAM-TOPUP-500", "provider": "a"}])
    pay(first)
    first = wait(first)
    assert first["status"] == "delivered", "Demo stock exhausted"
    seen = {first["code"]}
    try:
        for behavior in ("duplicate", "wrong_sku", "error_after_issue", "timeout_after_issue"):
            mode("a", behavior)
            order = create([{"sku": "STEAM-TOPUP-500", "provider": "a"}])
            pay(order)
            result = wait(order)
            assert result["status"] == "delivered", "Demo stock exhausted"
            assert result["code"] not in seen
            seen.add(result["code"])
            print(behavior, result["code"], result["money"], flush=True)
        reconciliation = api("GET", "/internal/reconciliation")
        assert all(row["balanced"] for row in reconciliation["balances"])
        print(json.dumps(reconciliation["provider_discrepancies"], ensure_ascii=False, indent=2))
    finally:
        mode("a", "normal")


def burst():
    # Existing admissions are counted too; run this on a fresh instance/database
    # or wait a minute after other demos. Completion may take two minutes.
    mode("a", "normal", 2)
    unpaid = create([{"sku": "STEAM-TOPUP-500", "provider": "a"}])
    orders = [create([{"sku": "STEAM-TOPUP-500", "provider": "a"}]) for _ in range(5)]
    try:
        for order in orders:
            pay(order)
        deadline = time.monotonic() + 190
        while time.monotonic() < deadline:
            queue = api("GET", "/internal/queue")
            print(json.dumps(queue, ensure_ascii=False), flush=True)
            states = [api("GET", "/api/v1/orders/" + order["id"]) for order in orders]
            if all(order["status"] == "delivered" for order in states):
                break
            time.sleep(5)
        else:
            raise RuntimeError("queue did not drain; check inventory")
        assert api("GET", "/api/v1/orders/" + unpaid["id"])["status"] == "created"
        for order in orders:
            wait(order)
    finally:
        mode("a", "normal")


def history():
    mode("a", "normal")
    start = datetime.now(timezone.utc).isoformat()
    order = create([{"sku": "STEAM-TOPUP-500", "provider": "a"}])
    before = datetime.now(timezone.utc).isoformat()
    pay(order)
    wait(order)
    old = api("GET", "/api/v1/orders/" + order["id"] + "?" + urllib.parse.urlencode({"at": before}))
    assert old["status"] == "created" and old["money"]["paid"] == 0
    report = api("GET", "/internal/reports?" + urllib.parse.urlencode(
        {"from": start, "to": datetime.now(timezone.utc).isoformat()}))
    for row in report["currencies"]:
        assert row["opening_pending"] + row["paid"] == row["delivered"] + row["refunded"] + row["closing_pending"]
    print(json.dumps({"historical_order": old, "report": report}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("partial", "dishonest", "burst", "history"))
    args = parser.parse_args()
    globals()[args.scenario]()
