#!/usr/bin/env bash
set -euo pipefail

base_url="http://localhost:8001"

wait_for_terminal_state() {
  local order_id="$1"
  local state=""
  for _ in $(seq 1 50); do
    state=$(curl -fsS "$base_url/orders/$order_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
    if [[ "$state" == "completed" || "$state" == "compensated" || "$state" == "failed" || "$state" == "compensation_failed" ]]; then
      curl -fsS "$base_url/orders/$order_id" | python3 -m json.tool
      return 0
    fi
    sleep 0.2
  done
  echo "Saga did not reach a terminal state (last state: $state)" >&2
  return 1
}

echo "Successful Saga"
success_id=$(curl -fsS -X POST "$base_url/orders" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-success' \
  -d '{"customer_id":"customer-1","sku":"BOOK-001","quantity":2,"amount_cents":4999,"shipping_address":"Pune, India"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')
wait_for_terminal_state "$success_id"

echo "Shipping failure with refund and inventory release"
failure_id=$(curl -fsS -X POST "$base_url/orders" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-shipping-failure' \
  -d '{"customer_id":"customer-2","sku":"BOOK-002","quantity":1,"amount_cents":2599,"shipping_address":"Mumbai, India","failure_at":"shipping"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')
wait_for_terminal_state "$failure_id"

