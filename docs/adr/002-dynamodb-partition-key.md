# ADR 002: DynamoDB primary key design for the orders table

- **Status:** Accepted
- **Date:** 2026-10-23

## Context

The Processor writes one item per order. The write path must be idempotent:
replaying the same batch (a retry, a manual DLQ replay, a Firehose
redelivery) must not create a second row for the same order.

Access patterns we need to support:

1. Look up one order by its code (Customer Service "track my order").
2. List all orders for a given day (Operations daily reconciliation).
3. Query orders by destination province within a day range (Regional analysis).

We estimated 10,000–50,000 orders per day, roughly 30,000 average.

## Decision

Composite primary key:

- `pk` = `ORDER#{order_code}`
- `sk` = `{created_date}` (YYYY-MM-DD)

The table also exposes a global secondary index:

- GSI `by_province_date`: `gsi1pk` = `to_province_id`, `gsi1sk` = `created_at`

## Rationale

**Idempotency.** The Processor uses a conditional put
(`attribute_not_exists(pk) AND attribute_not_exists(sk)`). A replayed order
fails the condition, is reported as a duplicate, and costs one failed write
request. This is the mechanism that satisfies the "send the same order 10
times, get 1 row" acceptance test.

**Query pattern 1** is a direct `GetItem` — O(1), no scan.

**Query pattern 2** is awkward with this key: because `pk` contains the
order code, there is no way to query "all orders on day X" without a scan
or a FilterExpression on `sk`. With 30,000 orders a day, a scan is not
acceptable for a hot path.

**Resolution:** access pattern 2 is served from S3 curated and Athena
instead of DynamoDB. DynamoDB is the real-time lookup surface for a
specific order, not an analytics surface. This is a deliberate division of
responsibility, and it is why the architecture has both stores.

**Query pattern 3** cannot use the GSI as designed either, because
`gsi1pk` = `to_province_id` puts every order in the same province in one
partition. A province receiving 5,000 orders a day still fits in a single
partition's 10,000 item limit for a day, but a 30-day range query would
reach 150,000 items in one partition and hot-partition.

**Revised GSI:** `gsi1pk` = `to_province_id#{created_date}`,
`gsi1sk` = `created_at`. Each province-day is its own partition, so a
province-day query is a single `Query` and different days spread across
partitions.

## Consequences

- Daily "all orders" queries go to Athena over the curated layer, not
  DynamoDB. This is the right tool for that job anyway.
- Province analysis runs against the GSI for near-real-time freshness, and
  against Athena for historical trend work.
- The `pk`/`sk` naming is deliberate: it leaves room for other entity types
  (`ORDER#`, `STATUS#`) in the same table later without a migration.
- The conditional put means a legitimate status update to an already-written
  order would be rejected. Since each order is written once per ingest date
  and status history is a separate entity, this has not come up. If it does,
  the fix is `UpdateItem` with a version attribute, not dropping the
  condition — the condition is what makes the pipeline safe to replay.
