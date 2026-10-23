# ADR 003: Use Athena instead of Redshift for the analytics layer

- **Status:** Accepted
- **Date:** 2026-10-23

## Context

The original 12-week proposal included a Redshift cluster for the analytics
layer. The current timeline is ~5.5 weeks and the budget is $200 in AWS
credits, estimated at ~$65/month for the whole platform.

Data volume: 10,000–50,000 orders/day, 5–20 MB/day, 150–600 MB/month,
30 days raw and 90 days processed retention.

## Decision

Use **Amazon Athena** over Parquet in S3 as the query engine. No Redshift
cluster.

## Rationale

**Cost.** A Redshift RA3xl cluster runs at roughly $3.26/hour, about $2,300
per month running continuously, plus storage. That is more than 10x the
entire project budget. A smaller dc2.large is still ~$700/month. Athena is
serverless and priced per query; at our volume (a handful of dashboard
refreshes and ad-hoc queries per day) the cost is measured in single digits
per month.

**Scale fit.** The total processed dataset is under 2 GB even at the top of
the estimate. Athena has no problem with this. Redshift's advantage appears
at terabyte scale with concurrent heavy joins — not our case.

**Operational cost.** A cluster is something that needs patching, vacuuming,
sort-key tuning, workload management and monitoring. Athena is a managed
service with no administration. For a 5-person team on a 5.5-week timeline,
cluster administration is time not spent on the pipeline.

**Feature fit.** QuickSight has a native Athena connector, and the Parquet
files Firehose and Glue write are directly queryable.

**Trade-off accepted:** Athena is slower for repeated scans of the same
tables (it pays per query, with no result cache). A dashboard refreshing
every minute against a large table would be wasteful — we refresh on a
15-minute schedule, and repeated work is bounded by the byte-scanned cutoff
of 10 GB per query set on the workgroup.

**When to revisit:** if the processed dataset grows past ~50 GB, if we need
sub-second dashboards with many concurrent viewers, or if the budget grows to
cover a cluster. The migration path is Athena → Redshift Spectrum over the
same S3 Parquet data, which means the ETL layer does not change.

## Consequences

- No cluster to size, patch, or monitor. The cost line for analytics is
  ~$0.03/month.
- Dashboards must be designed around the byte-scanned cutoff and
  partition pruning (partition by `to_province_id` and date).
- We lose the option of materialized views and user-defined functions that
  Redshift provides. Nothing in the requirements needs them.
