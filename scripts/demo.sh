#!/usr/bin/env bash
# Terminal demo of the payment flow: starts the stack with docker compose, makes a payment,
# retries it, reuses its id with other details, then reads it back once it is processed.
# Needs docker and curl only. Stops and removes the stack (and its volumes) at the end.
#
#   PAYMENTS_API_KEY      key the app is started with, default demo-key-1234 (never printed)
#   COMPOSE_PROJECT_NAME  default demo-<random>, so it does not touch another local stack
#   DEMO_PAUSE            seconds to pause between steps, default 1
#   DEMO_TIMEOUT          seconds to wait for start-up, default 180
#   DEMO_KEEP=1           leave the stack running afterwards
set -euo pipefail

cd "$(dirname "$0")/.."

export PAYMENTS_API_KEY="${PAYMENTS_API_KEY:-demo-key-1234}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-demo-$(date +%s)-$RANDOM}"
DEMO_PAUSE="${DEMO_PAUSE:-1}"
DEMO_TIMEOUT="${DEMO_TIMEOUT:-180}"
BASE_URL=http://localhost:8080

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  BOLD=$'\e[1m' CYAN=$'\e[36m' GREEN=$'\e[32m' YELLOW=$'\e[33m' RED=$'\e[31m' DIM=$'\e[2m' RESET=$'\e[0m'
else
  BOLD='' CYAN='' GREEN='' YELLOW='' RED='' DIM='' RESET=''
fi

work=$(mktemp -d)
cleanup() {
  rc=$?
  if [ "${DEMO_KEEP:-0}" = 1 ]; then
    echo "${DIM}Stack left running (project $COMPOSE_PROJECT_NAME). Stop it with:${RESET}"
    echo "${DIM}  COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME docker compose down -v --rmi local${RESET}"
  else
    echo "${DIM}Stopping the stack and removing its volumes ...${RESET}"
    docker compose down -v --rmi local --remove-orphans >"$work/down.log" 2>&1 \
      || { cat "$work/down.log" >&2; rc=1; }
  fi
  rm -rf "$work"
  exit "$rc"
}
trap cleanup EXIT

step=0
heading() { step=$((step + 1)); echo; echo "${BOLD}${CYAN}$step. $*${RESET}"; }
cmd() { local line; for line in "$@"; do echo "${DIM}$line${RESET}"; done; }
say() { echo "${GREEN}=> $*${RESET}"; }
pause() { sleep "$DEMO_PAUSE"; }
fail() { echo "${RED}FAIL: $*${RESET}" >&2; exit 1; }

# Indents compact JSON without jq: a newline after each { [ , and before each } ], skipping
# anything inside strings. The problem "type" line (a long URL) is dropped to keep it narrow.
pretty() {
  awk '
    function pad(n,  r) { r = "  "; while (n-- > 0) r = r "  "; return r }
    {
      s = $0; out = pad(0); depth = 0; instr = 0; esc = 0
      for (i = 1; i <= length(s); i++) {
        c = substr(s, i, 1)
        if (instr) {
          out = out c
          if (esc) esc = 0; else if (c == "\\") esc = 1; else if (c == "\"") instr = 0
        } else if (c == "\"") { instr = 1; out = out c }
        else if (c == "{" || c == "[") { depth++; out = out c "\n" pad(depth) }
        else if (c == "}" || c == "]") { depth--; out = out "\n" pad(depth) c }
        else if (c == ",") { out = out c "\n" pad(depth) }
        else if (c == ":") { out = out ": " }
        else { out = out c }
      }
      print out
    }' "$1" | grep -v '"type": '
}

# api METHOD PATH [BODY]: calls the API, leaves the body in $work/body, prints and returns the status.
api() {
  local args=(-s -o "$work/body" -w '%{http_code}' -X "$1" -H "X-API-Key: $PAYMENTS_API_KEY")
  [ $# -lt 3 ] || args+=(-H "Content-Type: application/json" -d "$3")
  curl "${args[@]}" "$BASE_URL$2"
}
field() { sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p" "$work/body" | head -n 1; }

show_balance() {
  cmd "\$ curl -s localhost:8080/accounts/$1 -H 'X-API-Key: ****'"
  [ "$(api GET "/accounts/$1")" = 200 ] || fail "GET /accounts/$1: $(cat "$work/body")"
  echo "  $(cat "$work/body")"
}

echo "${BOLD}Payment service demo${RESET}  ${DIM}(project $COMPOSE_PROJECT_NAME)${RESET}"
echo "${DIM}Starting Postgres, Kafka and the app with docker compose ...${RESET}"
docker compose up -d --build --wait >"$work/up.log" 2>&1 || { cat "$work/up.log" >&2; fail "docker compose up"; }
# Health and info are open but metrics need the key; a keyed 200 for a seeded account is the readiness check.
deadline=$((SECONDS + DEMO_TIMEOUT))
until [ "$(api GET /accounts/ACC-1001 2>/dev/null)" = 200 ]; do
  [ "$SECONDS" -lt "$deadline" ] || { docker compose logs app | tail -n 30 >&2; fail "app not up after ${DEMO_TIMEOUT}s"; }
  sleep 2
done
echo "${DIM}App is up.${RESET}"
pause

payment_id="pay-$RANDOM$RANDOM"
request="{\"paymentId\":\"$payment_id\",\"fromAccount\":\"ACC-1001\",\"toAccount\":\"ACC-1002\",\"amount\":25.00,\"currency\":\"USD\"}"
changed="{\"paymentId\":\"$payment_id\",\"fromAccount\":\"ACC-1001\",\"toAccount\":\"ACC-1002\",\"amount\":99.00,\"currency\":\"USD\"}"
post_cmd() {
  cmd "\$ curl -s -X POST localhost:8080/payments -H 'X-API-Key: ****' \\" \
      "    -H 'Content-Type: application/json' \\" \
      "    -d '{\"paymentId\":\"$payment_id\",\"fromAccount\":\"ACC-1001\",\"toAccount\":\"ACC-1002\"," \
      "         \"amount\":$1,\"currency\":\"USD\"}'"
}

heading "Two seeded accounts and their balances"
show_balance ACC-1001
show_balance ACC-1002
pause

heading "Pay 25.00 USD from ACC-1001 to ACC-1002 (paymentId is the idempotency key)"
post_cmd 25.00
status=$(api POST /payments "$request")
[ "$status" = 201 ] || fail "POST /payments returned $status: $(cat "$work/body")"
echo "${YELLOW}HTTP $status Created${RESET}"
pretty "$work/body"
say "accepted as $(field status); Kafka processes it in the background"
pause

heading "Send the same request again (a client retry)"
post_cmd 25.00
status=$(api POST /payments "$request")
[ "$status" = 409 ] || fail "retry returned $status, expected 409: $(cat "$work/body")"
echo "${YELLOW}HTTP $status Conflict${RESET}"
pretty "$work/body"
say "409 Conflict: recognised as a retry, the original payment is returned, no second charge"
pause

heading "Reuse the same paymentId with a different amount"
post_cmd 99.00
status=$(api POST /payments "$changed")
[ "$status" = 422 ] || fail "reused id returned $status, expected 422: $(cat "$work/body")"
echo "${YELLOW}HTTP $status Unprocessable Content${RESET}"
pretty "$work/body"
say "422: same id, different details, rejected; nothing is created or charged"
pause

heading "Read the payment back and check the balances"
deadline=$((SECONDS + 30))
while :; do
  [ "$(api GET "/payments/$payment_id")" = 200 ] || fail "GET /payments/$payment_id: $(cat "$work/body")"
  case "$(field status)" in RECEIVED|PROCESSING) ;; *) break ;; esac
  [ "$SECONDS" -lt "$deadline" ] || fail "payment still $(field status) after 30s"
  sleep 1
done
cmd "\$ curl -s localhost:8080/payments/$payment_id -H 'X-API-Key: ****'"
echo "${YELLOW}HTTP 200 OK${RESET}"
pretty "$work/body"
[ "$(field status)" = PROCESSED ] || fail "payment ended as $(field status)"
say "settled: status PROCESSED"
show_balance ACC-1001
show_balance ACC-1002
say "ACC-1001 paid 25.00 once, ACC-1002 received 25.00 once"
pause

echo
echo "${BOLD}${GREEN}Demo finished.${RESET}"
