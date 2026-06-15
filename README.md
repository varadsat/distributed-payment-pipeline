# Payment Transaction Intake & Settlement Pipeline

High-throughput, event-driven payment ingestion in Go. Payments arrive from
multiple rails (card, UPI, bank transfer, wallet) over gRPC, get normalized,
deduplicated, and durably persisted, then fan out to independent consumers
(ledger, fraud, notifications, settlement) via Kafka.

The two correctness guarantees this project is built to demonstrate:
- **Never double-process** a retried submission (Redis idempotency).
- **Never lose** an acknowledged payment event (transactional outbox + relay).

## Stack
Go · gRPC/protobuf · Kafka (Redpanda locally) · Redis · Postgres · Terraform

## Architecture
```
sources --gRPC--> intake --> Redis (idempotency)
                       \--> Postgres (txn + outbox, one tx)
                                  |
                            outbox relay --Kafka--> payments.received (key: account_id)
                                                       |-> ledger (double-entry)
                                                       |-> fraud
                                                       |-> notifications
                                                       \-> settlement --> payments.settled / payouts.requested
```

## Quick start
```bash
make up            # start postgres, redis, redpanda (+ console at :8080)
make migrate-up    # apply SQL migrations (set DATABASE_URL)
make proto         # generate Go from protobuf
make build         # build all binaries into ./bin
```

## Layout
- `api/proto` — gRPC contract (source of truth for the API)
- `cmd/*` — one binary per process (intake, relay, consumers, settlement, dlqctl)
- `internal/*` — implementation packages (domain, intake, normalize, validate,
  idempotency, store, outbox, kafka, ledger, settlement, observability, config)
- `internal/store/migrations` — SQL schema
- `deploy/terraform` — AWS IaC (optional)
- `test/load` — load + chaos tests

See `BUILD_PLAN.md` for the chunked build order.
