#!/usr/bin/env bash
set -euo pipefail
echo "Restart the API with PROVIDER_A_ERROR_RATE=1 PROVIDER_B_ERROR_RATE=0, then run:"
echo "  SKU=STEAM-TOPUP-500 ./scripts/race.sh"
echo "Expect delivered.provider = b and a single code."
echo
echo "Or run the Go test (no restart needed):"
echo "  go test ./cmd/api -count=1 -run TestFallback"
