#!/usr/bin/env bash
set -euo pipefail
BASE="${BASE_URL:-http://127.0.0.1:8080}"
SKU="${SKU:-STEAM-TOPUP-500}"

oid="$(python3 - "$BASE" "$SKU" <<'PY'
import json, sys, urllib.request
base, sku = sys.argv[1], sys.argv[2]
req = urllib.request.Request(base + "/api/v1/orders", data=json.dumps({"sku": sku}).encode(),
                             headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(req) as r:
    print(json.load(r)["id"])
PY
)"
echo "order $oid"

python3 - "$BASE" "$oid" <<'PY'
import json, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor
base, oid = sys.argv[1], sys.argv[2]
body_order = json.dumps({"sku": "x"}).encode()

def send(i):
    payload = json.dumps({
        "event_id": f"evt_{oid}_{i}",
        "order_id": oid,
        "status": "paid",
        "amount": 500,
        "currency": "RUB",
        "created_at": "2025-01-01T12:00:00Z",
    }).encode()
    req = urllib.request.Request(base + "/webhook/payment", data=payload,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as r:
        r.read()

with ThreadPoolExecutor(max_workers=50) as ex:
    list(ex.map(send, range(50)))
print("sent 50 webhooks")
PY

python3 - "$BASE" "$oid" <<'PY'
import json, sys, time, urllib.request
base, oid = sys.argv[1], sys.argv[2]
for _ in range(50):
    with urllib.request.urlopen(base + "/api/v1/orders/" + oid) as r:
        o = json.load(r)
    print("status", o["status"])
    if o["status"] == "delivered":
        print(json.dumps(o, ensure_ascii=False, indent=2))
        if not o.get("code"):
            raise SystemExit("delivered without code")
        raise SystemExit(0)
    time.sleep(0.2)
raise SystemExit("not delivered")
PY
