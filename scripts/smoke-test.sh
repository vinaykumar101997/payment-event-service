#!/usr/bin/env bash
# Smoke test against a running app (CI runs it against the Docker image started with
# docker-compose). Waits until the app answers, then checks the API key and one payment.
#
#   PAYMENTS_API_KEY  the key the app was started with (required)
#   BASE_URL          default http://localhost:8080
#   SMOKE_TIMEOUT     seconds to wait for start-up, default 90
set -euo pipefail

: "${PAYMENTS_API_KEY:?set PAYMENTS_API_KEY to the key the app was started with}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-90}"

fail() { echo "FAIL: $*" >&2; exit 1; }

# /actuator/health would only show the app is up. A 200 for a seeded account (V2) with the key
# also shows migrations ran and authentication works.
echo "Waiting up to ${SMOKE_TIMEOUT}s for ${BASE_URL} ..."
deadline=$((SECONDS + SMOKE_TIMEOUT))
until [ "$(curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: $PAYMENTS_API_KEY" "$BASE_URL/accounts/ACC-1001")" = 200 ]; do
  [ "$SECONDS" -lt "$deadline" ] || fail "app did not answer 200 on /accounts/ACC-1001 within ${SMOKE_TIMEOUT}s"
  sleep 2
done
echo "App is up after ${SECONDS}s"

payment_id="smoke-$(date +%s)-$RANDOM"
body=$(mktemp)
trap 'rm -f "$body"' EXIT

status=$(curl -s -o "$body" -w '%{http_code}' -X POST "$BASE_URL/payments" \
  -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" \
  -d "{\"paymentId\": \"$payment_id\", \"fromAccount\": \"ACC-1001\", \"toAccount\": \"ACC-1002\", \"amount\": 1.00, \"currency\": \"USD\"}")
[ "$status" = 201 ] || fail "POST /payments returned $status: $(cat "$body")"
grep -q "\"paymentId\":\"$payment_id\"" "$body" || fail "POST response has no paymentId $payment_id: $(cat "$body")"
grep -q '"status":"RECEIVED"' "$body" || fail "POST response status is not RECEIVED: $(cat "$body")"
echo "POST /payments -> 201 RECEIVED ($payment_id)"

status=$(curl -s -o "$body" -w '%{http_code}' -H "X-API-Key: $PAYMENTS_API_KEY" "$BASE_URL/payments/$payment_id")
[ "$status" = 200 ] || fail "GET /payments/$payment_id returned $status: $(cat "$body")"
grep -q "\"paymentId\":\"$payment_id\"" "$body" || fail "GET response is not payment $payment_id: $(cat "$body")"
echo "GET /payments/$payment_id -> 200 $(grep -o '"status":"[A-Z]*"' "$body")"

status=$(curl -s -o "$body" -w '%{http_code}' "$BASE_URL/payments/$payment_id")
[ "$status" = 401 ] || fail "GET without X-API-Key returned $status, expected 401"
grep -q '"status":401' "$body" || fail "401 body is not a problem response: $(cat "$body")"
echo "GET without X-API-Key -> 401"

status=$(curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: wrong-$PAYMENTS_API_KEY" "$BASE_URL/payments/$payment_id")
[ "$status" = 401 ] || fail "GET with a wrong X-API-Key returned $status, expected 401"
echo "GET with a wrong X-API-Key -> 401"

echo "Smoke test passed"
