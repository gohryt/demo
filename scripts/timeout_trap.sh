#!/usr/bin/env bash
set -euo pipefail
echo "Restart the API with PROVIDER_A_TIMEOUT_RATE=1 PROVIDER_TIMEOUT=300ms, then run:"
echo "  ./scripts/race.sh"
echo "Expect delivered.provider = a (retry same request_id, no fallback to B) and a single code."
echo
echo "Or run the Go test (no restart needed):"
echo "  go test ./cmd/api -count=1 -run TestTimeoutSameCode"
