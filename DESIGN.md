# MARS Cosmetics — Order & Inventory Sync Service

## 1. Decision summary

Use a PostgreSQL-backed inventory ledger as the source of truth, with a transactional order-ingestion API and an outbox-driven set of channel/ERP workers. Redis is used for queues, rate limiting and short-lived locks; it is never the authority for stock. Each channel has an isolated adapter and retry queue, so a failing marketplace cannot block order intake or the other stock feeds.

The key concurrency rule is an atomic conditional decrement in PostgreSQL. A reservation succeeds only when `available_qty >= requested_qty`; therefore two buyers cannot both reserve the last unit. A reservation, order state change and outbox event are committed in one database transaction.

See [architecture.mmd](architecture.mmd).

## 2. Architecture and flow

The NestJS service runs as stateless API/worker containers on AWS ECS Fargate behind an ALB. PostgreSQL (RDS Multi-AZ) stores orders, inventory, reservations, outbox events and the audit ledger. Redis (ElastiCache) provides BullMQ queues, distributed rate-limit counters and deduplication TTLs. Channel adapters normalize inbound events and isolate outbound API behavior.

1. Website/Nykaa/Blinkit webhooks enter the webhook API; Flipkart polling is scheduled every two minutes.
2. Each adapter authenticates, validates and normalizes an event, then submits it to the order command handler. The external `(channel, external_order_id, event)` is idempotent.
3. In one PostgreSQL transaction, the service creates/updates the order, atomically reserves or releases every item, writes an inventory ledger row, and inserts outbox records.
4. Outbox workers publish inventory-dirty and ERP-order jobs to Redis. Workers claim jobs with leases and retry using exponential backoff plus jitter.
5. Channel stock workers coalesce dirty SKUs for up to a few seconds, then call the channel-specific API under its rate limit. A successful push records the channel projection/version. ERP workers send confirmed orders with a stable idempotency key.
6. The operations screen reads failed jobs, attempts, next retry time and last error from PostgreSQL; retry creates a new attempt without deleting the audit history.

## 3. Data model

All IDs are UUIDs; timestamps are UTC (`timestamptz`). Quantities are integers and never negative.

| Table | Important columns and constraints |
|---|---|
| `orders` | `id`, `channel`, `external_order_id`, status, event version/time, totals; unique `(channel, external_order_id)` |
| `order_items` | `order_id`, `sku`, `qty`, price; primary key `(order_id, sku)`; `qty > 0` |
| `inventory` | `sku` primary key, `on_hand_qty`, `reserved_qty`, version; check both >= 0 and `reserved_qty <= on_hand_qty` |
| `reservations` | `order_id`, `sku`, qty, status, released_at; unique `(order_id, sku)` |
| `inventory_ledger` | immutable `sku`, delta, reason, reference type/id, before/after quantities, created_at; indexed by `(sku, created_at)` |
| `channel_stock_projection` | `(channel, sku)` primary key, desired/last-pushed qty, inventory version, pushed_at, error |
| `outbox_events` | aggregate key, event type/payload, status, attempts, available_at, lease; unique `dedupe_key` |
| `inbound_events` | channel, external event id, payload hash, received_at; unique `(channel, external_event_id)` |

Duplicate orders are rejected by the database unique key, not by a cache. Duplicate deliveries return the already-known order result. Out-of-order events are accepted only if their channel sequence/event time is newer than the stored state; a cancellation cannot release a reservation twice because release is an idempotent state transition.

## 4. Ingestion, reservation and consistency

Webhook handlers acknowledge after durable receipt/validation, not after calling a marketplace. Flipkart’s cursor is stored transactionally with the poll result; overlapping polls are prevented by a scheduler lease. A channel outage pauses only that adapter, with cursor-based replay where available and a visible lag alert. Website cancellation and marketplace cancellation both use the same release command.

Reservation pseudo-code (inside a PostgreSQL transaction):

```text
for item in order.items sorted by sku:
  updated = UPDATE inventory
    SET reserved_qty = reserved_qty + item.qty, version = version + 1
    WHERE sku = item.sku
      AND on_hand_qty - reserved_qty >= item.qty
    RETURNING on_hand_qty - reserved_qty AS available_after, version

  if updated is empty:
    ROLLBACK                         // no partial reservation
    mark order REJECTED_NO_STOCK
    emit audit/result (also transactionally)
  else:
    INSERT reservation(order_id, sku, qty, ACTIVE)
    INSERT inventory_ledger(..., reason = ORDER_RESERVED)

COMMIT; enqueue ERP and stock outbox events from the same transaction
```

If two channels request the last unit, both transactions lock the same `inventory` row through the conditional update. One commits first and changes available stock to zero; the other updates zero rows and is rejected. It cannot oversell, and the losing channel receives the next stock projection. A small safety buffer can be configured per channel, but is not used to hide an inventory inconsistency.

## 5. Stock push and failure handling

Every reservation/release increments the SKU version and creates a dirty projection for all four channels. Workers coalesce repeated changes and always push the latest desired value (not every intermediate value). Normal-load target is p95 under 60 seconds from commit to successful push.

* Website: direct DB/API writer, bounded connection pool and short transaction.
* Nykaa: one-SKU jobs, token bucket at 4 requests/sec (below the 5/sec limit).
* Flipkart: batches of up to 50 SKUs, token bucket at 8 requests/sec; 429 honors `Retry-After` and requeues the batch.
* Blinkit: urgent REST jobs at 1–2 requests/sec with a 10-second timeout, 3 quick retries, then durable backoff. The daily CSV is the reconciliation path; it is not relied on for the 60-second urgent path.

Each adapter has a circuit breaker and independent queue. Retryable HTTP/network errors use exponential backoff (for example 10s, 30s, 2m, 10m, then hourly). Permanent validation/auth errors go to a dead-letter queue after a bounded number of attempts. Ops can inspect and retry a single failure. A reconciliation job periodically compares channel projections with the desired stock and repairs missed pushes.

ERP downtime is safe because confirmed orders remain in the outbox until acknowledged. ERP requests use `idempotency_key = order.id`; the adapter marks `SENT` only after a successful response. On timeout it retries the same key and then reconciles by key/order reference before retrying. This gives exactly-once ERP effect assuming Business Central honors the idempotency key or exposes lookup-by-order-reference; otherwise the honest guarantee is at-least-once delivery with duplicate detection during reconciliation.

## 6. Observability and operations

Day-one metrics/alerts:

1. order intake rate, processing latency and `NO_STOCK` rate (alert if sale-day cancellation exceeds 0.5%);
2. inventory-to-channel push age by channel/SKU (alert at 45s warning, 60s critical);
3. outbox depth, oldest event age and dead-letter count;
4. ERP pending age and failed/timeout rate;
5. reservation invariant violations, duplicate-event rate and database lock/deadlock rate.

Structured logs carry `order_id`, channel, external ID, SKU, outbox ID and correlation ID. The immutable ledger answers “why did SKU X change?” without relying on logs.

## 7. Assumptions, scope and trade-offs

### Assumptions

* Each channel provides a stable event ID or order ID; where it does not, the adapter derives a deterministic hash from channel/order/version.
* Channel stock means sellable stock: `on_hand - active reservations`; warehouse adjustments arrive as authenticated admin commands.
* A confirmed order is one whose reservation transaction succeeds. Payment/refund policy remains owned by the channel/ERP.
* Business Central supports an idempotency key or query by external order reference. Credentials use AWS Secrets Manager; all APIs use HTTPS.
* SKU catalog and prices are mastered elsewhere and supplied before order ingestion.
* RDS/ElastiCache, ECS and CloudWatch fit the approximate monthly budget; one production region and managed backups are sufficient for v1.

### Deliberately out of v1

Multi-warehouse allocation, advanced ATP/forecasting, automatic catalog synchronization, payment capture, returns workflow beyond stock release, and an LLM ops assistant. They add domain and operational risk without solving the immediate oversell problem. A second warehouse would add `warehouse_inventory`, allocation rules and warehouse-aware reservations rather than changing the order/outbox pattern.

### Trade-offs

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Stock authority | PostgreSQL row + immutable ledger | Redis-only counters | Durable auditability and transactional reservation; Redis alone risks loss/inconsistent recovery. |
| Async delivery | Transactional outbox + Redis workers | Direct synchronous fan-out in webhook request | Keeps channels independent and survives ERP downtime; adds worker/retry complexity. |
| Concurrency | Atomic conditional update | Distributed lock around the whole order | Database constraint is simpler and lock scope is one SKU; sorted SKUs reduce deadlocks. |
| Channel freshness | Coalesced latest-state pushes | Send every intermediate inventory event | Meets the 60s goal with less API pressure; reconciliation handles missed pushes. |

### AI usage note

AI was used lightly to review the requirements and help draft the architecture diagram and document structure; all capacity, consistency, failure-mode and trade-off decisions were reasoned through and are defendable by the author.
