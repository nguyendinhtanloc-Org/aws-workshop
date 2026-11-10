# ADR 005: Amazon Athena v3 Serverless MERGE over Scheduled AWS Glue PySpark

- **Status:** Accepted
- **Date:** 2026-11-10

## Context
The platform processes ~30,000 orders/day (~1,250 records/hour) into Apache Iceberg tables.
Running an AWS Glue 5.0 PySpark cluster (minimum 2 DPUs, 2–3 minutes container startup overhead) hourly or daily incurs unnecessary compute costs (\$30–\$60/month) for micro-batches containing only a few thousand rows.

## Decision
Use **Amazon Athena Engine v3** to execute native SQL `MERGE INTO` operations on Apache Iceberg tables registered in the AWS Glue Data Catalog, orchestrated by **AWS Step Functions**.

## Rationale
1. **Serverless Economics:** Athena v3 charges \$5.00 per TB scanned. Merging ~1,250 records with partition pruning scans only tens of megabytes per run $\to$ cost is under \$2.00/month.
2. **Native Iceberg DML:** Athena v3 supports full Trino-compatible Iceberg DML (`MERGE INTO`, `UPDATE`, `OPTIMIZE`, `VACUUM`).
3. **No Cluster Cold-Start:** Zero container spin-up overhead.

## Consequences
- **Positive:**
  - Saves $>90\%$ in Lakehouse compute costs.
  - Zero cluster management.
- **Negative:**
  - Local development harness must use Trino (Presto) container instead of Spark to match SQL dialect.
  - Query execution timeout is capped at 30 minutes (plenty for hourly micro-batches).

