# Operations Runbook

Who this is for: anyone on call for the GHN pipeline, including people who
did not build it.

## Where things are

| What | Where to find it |
|---|---|
| Dashboard | CloudWatch → Dashboards → `GHN-Pipeline-Ops` |
| Alerts | SNS topic exported by the monitoring stack |
| Logs | CloudWatch → Logs → `/aws/lambda/<stack>-ingestion-Ingestor` and `-processing-Processor` |
| Failed records | SQS DLQ, URL exported by the ingestion / processing stack |
| Raw data | S3 `raw/` (30-day retention) |
| Data warehouse | Athena, workgroup `ghn-analytics` |
| Dashboards | QuickSight |

## Seeding secrets

The GHN API token is never in the repository. To (re)seed it:

```bash
aws secretsmanager put-secret-value \
  --secret-id ghn-analytics-dev-ingestion-ghn-api \
  --secret-string '{"token":"<token>"}'
```

If ingestion shows zero records for 15 minutes after seeding, restart the
schedule or invoke the function manually.

## Order watchlist and webhooks

The GHN API has **no list-orders endpoint**: `v2/shipping-order/detail`
requires an `OrderCode`, and order codes only exist once an order is
created (`v2/shipping-order/create`). The Ingestor therefore polls a
watchlist in `GHN_ORDER_CODES` (comma-separated codes, in the Ingestor
Lambda's environment or wherever you manage that variable).

- **Demo/CI runs:** `python3 scripts/create_demo_order.py` creates test
  orders through the create route and prints the exact
  `GHN_ORDER_CODES=...` line to paste into `.env`. Its
  `client_order_code` is timestamped, so re-running never duplicates an
  order (GHN returns the previously created code for a reused
  `client_order_code`).
- **Live shop runs:** append the shop's real order codes as sales
  happen; each scheduled run fetches the current detail for every code.

Status *history* is not polled: in production, GHN's **Order Status
Callback (webhook)** pushes status changes to an HTTPS endpoint. Until a
webhook receiver is deployed, the Ingestor derives one `shipping_status`
record per polled order (its current status), so the pipeline keeps the
three-source contract (`master_data`, `orders`, `shipping_status`). When
a webhook receiver is added, it writes directly to the raw/curated store
and the derived records become redundant.

Ward master data is fetched per district (the ward endpoint requires
`district_id`); `GHN_WARD_DISTRICTS` bounds that lookup to the districts
worth keeping.

## Alerts

### Delivery success rate dropped below 90%

**Meaning:** more than 1 in 10 orders is failing or not delivered. Operations
is feeling it.

1. Open Athena and run the daily KPI query for the last 3 days:

   ```sql
   SELECT order_date, total_orders, delivered_orders, success_rate
   FROM ghn_analytics.gold_daily_kpis
   WHERE order_date >= current_date - interval '3' day
   ORDER BY order_date;
   ```

2. Break it down by province:

   ```sql
   SELECT to_province_id, COUNT(*) AS orders, AVG(success_rate) AS rate
   FROM ghn_analytics.gold_province_kpis
   WHERE to_province_id IN ( /* provinces from step 1 */ )
   GROUP BY to_province_id
   ORDER BY rate;
   ```

3. If one province dominates the failure, it is almost certainly a carrier
   issue, not a pipeline issue. Verify the pipeline is healthy (check
   ingestion lag below) before escalating to Operations.

4. If failures are spread across all provinces and the raw ingest rate also
   dropped, the pipeline itself may be at fault. Check the ingest error rate
   metric and the DLQ depth.

### Ingestion lag exceeded 5 minutes

1. Check the EventBridge rule `IngestSchedule` is enabled.
2. Check the last Lambda invocation: any errors? Check for
   `Rate limited` or `request error` in the logs — the client retries with
   backoff, so a burst of these means we hit the GHN API rate limit.
3. If the API is rate limiting us, the Ingestor is already backing off. Wait
   15 minutes. If it does not recover, reduce the schedule to every 10
   minutes temporarily and note it in the team channel.
4. Check DLQ depth. A non-zero depth with a full Ingestor queue means the
   API is failing, not the pipeline.

### Failed rate above 10%

Same procedure as the success-rate alert, but the pipeline is healthy
(check ingestion rate is normal first). Escalate to Operations with the
province breakdown.

### Lead time spiked above 2x the 30-day average

1. Check whether it is a single province or global.
2. Global + ingestion healthy → likely a carrier capacity issue during peak
   hours. Confirm with the hourly distribution.
3. Provincial → check whether that province also shows elevated failed rate.

## Replaying failed records

Never delete from the DLQ by hand until you understand why the record
failed.

```bash
# 1. Inspect first — dry run does not delete
python scripts/replay_dlq.py <dlq-url> --dry-run

# 2. Fix the root cause (schema change, validation rule, transient API error)

# 3. Replay
python scripts/replay_dlq.py <dlq-url> --limit 100
```

The replay script deletes messages from the DLQ as it processes them. Run it
with `--dry-run` first and read the output.

## Backfilling historical data

Backfills never run inside the scheduled pipeline.

```bash
python scripts/backfill.py --source orders --days 7 --dry-run
python scripts/backfill.py --source orders --days 7
```

## Deploying a change

```bash
ruff check src cdk tests scripts && mypy src cdk tests && python3 run_tests.py
python3 scripts/build_lambdas.py          # bake pip deps into build/lambda-*
cdk diff --app "python cdk/app.py --minimal"   # review (minimal: skip Glue/Athena/monitoring, saves ~$33/mo NAT)
cdk deploy --all --app "python cdk/app.py --minimal" --require-approval never
```

Drop `--minimal` to include the analytics (Glue + Athena) and monitoring stacks.
To roll back a bad deploy: `cdk deploy --all --app "python cdk/app.py"` after
reverting the commit, or use the CloudFormation console's rollback action.
Raw and curated data are not destroyed by a stack rollback (both buckets use
RETAIN policy). The GHN secret and DynamoDB table also use RETAIN; delete the
secret manually after `cdk destroy` so the token is actually gone.

## Idempotency check

To confirm the Processor is not double-writing:

```bash
aws dynamodb scan --table-name <orders-table> \
  --projection-expression 'order_code' --select COUNT_DISTINCT
```

The count of distinct order codes should match the total item count. If not,
a conditional put was lost — investigate the writer before proceeding.
