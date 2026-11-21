# FastAPI + RabbitMQ Saga Pattern

This repository is a deliberately small, production-shaped example of an
**orchestrated Saga**. The Order Service owns the state machine. It sends commands
through RabbitMQ and reacts to events from three independent participant services.

## Architecture

```text
                         RabbitMQ
                    commands      events
                       │             ▲
                       ▼             │
Client ─HTTP─> Order Service / Saga Orchestrator
                       │             ▲
           ┌───────────┼─────────────┐
           ▼           ▼             ▼
      Inventory     Payment       Shipping
       Service      Service        Service
```

The happy path is:

```text
ReserveInventory → InventoryReserved
ChargePayment    → PaymentCharged
ScheduleShipment → ShipmentScheduled → completed
```

If shipping fails, already completed work is compensated in reverse order:

```text
ShipmentRejected → RefundPayment → PaymentRefunded
                  → ReleaseInventory → InventoryReleased → compensated
```

Use `failure_at: "finalization"` to see all three compensations, including shipment
cancellation. Other supported failure points are `inventory`, `payment`, and `shipping`.

## Run it

```bash
docker compose up --build -d
docker compose ps
./scripts/run_demo.sh
```

Endpoints and interactive documentation:

| Component | URL |
|---|---|
| Order orchestrator | http://localhost:8001/docs |
| Inventory | http://localhost:8002/docs |
| Payment | http://localhost:8003/docs |
| Shipping | http://localhost:8004/docs |
| RabbitMQ management (`saga` / `saga`) | http://localhost:15672 |

Create an order manually:

```bash
curl -X POST http://localhost:8001/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-request-1' \
  -d '{
    "customer_id": "customer-1",
    "sku": "BOOK-001",
    "quantity": 2,
    "amount_cents": 4999,
    "shipping_address": "Pune, India",
    "failure_at": "shipping"
  }'
```

The POST returns `202 Accepted`. Poll `GET /orders/{order_id}` because message-driven
work is asynchronous. Inspect participant state with `GET /reservations`,
`GET /payments`, and `GET /shipments` on their respective ports.

Stop and remove the demo containers with:

```bash
docker compose down --remove-orphans
```

## Why the code looks this way

- Commands express intent (`ChargePayment`); events state facts (`PaymentCharged`).
- Every envelope has message, Saga, correlation, and causation IDs.
- Durable queues, persistent messages, publisher confirms, bounded retries, and DLQs
  demonstrate at-least-once delivery mechanics.
- Consumer inboxes and domain-level idempotency prevent repeated side effects.
- The orchestrator records every step and compensates only completed work.
- Participant APIs are read-only: business writes arrive through commands.
- Money is represented as integer cents, avoiding floating-point rounding errors.

## Important production boundary

The requested databases are in memory, so restarting a service intentionally loses its
state. The inbox also resets. This makes the example easy to run, but it cannot provide
crash durability.

In production, each service should use its own durable database and atomically commit
its domain change together with an **outbox** record. An outbox relay then publishes the
message; consumers atomically store an **inbox** record with their domain change. This
closes the database/broker dual-write gap that an in-memory demo cannot truthfully solve.
PostgreSQL plus an outbox relay (or CDC such as Debezium) is a common next step.

For long-running, high-value workflows, also consider a durable workflow engine such as
Temporal. The same actions and compensations remain, while the engine persists timers,
retries, and workflow state.

## Development checks

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pytest
ruff check .
ruff format --check .
```
