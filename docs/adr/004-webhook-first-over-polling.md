# ADR 004: Webhook-First Ingestion over Watchlist Polling

- **Status:** Accepted
- **Date:** 2026-11-10

## Context
In an e-commerce / merchant logistics pipeline processing ~30,000 orders/day with an average delivery lifecycle of 2–3 days, there are 60,000–90,000 active, open orders at any given moment.
Polling the GHN API (`POST /shipping-order/detail`) every 5 minutes for open orders would require 17–26 million API requests per day (~200–300 calls/second). This is completely unsustainable:
1. Causes immediate HTTP 429 rate-limiting from GHN.
2. Exceeds AWS Lambda's 15-minute maximum execution timeout.
3. Wastes massive amounts of compute and network egress.

## Decision
Adopt a **Webhook-First (Event-Driven)** architecture with **Daily Reconciliation Polling**:
1. **Primary Ingestion:** GHN Webhook callbacks (`create`, `switch_status`, `update_weight`) are received by Amazon API Gateway, authenticated via HMAC signature, and enqueued into an Amazon SQS FIFO Queue with `MessageGroupId = order_code`.
2. **Reconciliation Polling:** An EventBridge cron triggers a reconciliation Lambda once per day at 03:00 AM to poll open orders from the watchlist and reconcile any missed webhook callbacks.

## Consequences
- **Positive:**
  - Data freshness drops from ~6 minutes down to $< 5$ seconds.
  - SQS FIFO guarantees strict in-order processing per `order_code`.
  - Ingestion cost drops from hundreds of dollars to under \$1.50/month.
- **Negative:**
  - Requires public webhook endpoint with HTTPS and HMAC verification.
  - Requires fallback reconciliation worker for network outages between GHN and AWS.

