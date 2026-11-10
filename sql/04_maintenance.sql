-- ============================================================================
-- Step 3: Iceberg Table Maintenance (Scheduled Daily at 02:00 AM)
-- Optimizes file sizes (bin-pack) and expires historical snapshots (VACUUM)
-- ============================================================================

-- [AWS Athena Engine v3 Syntax]
-- 1. Bin-pack compaction for orders table
OPTIMIZE ghn_analytics.orders REWRITE DATA USING BIN_PACK;

-- 2. Bin-pack compaction for order_events table
OPTIMIZE ghn_analytics.order_events REWRITE DATA USING BIN_PACK;

-- 3. Vacuum expired snapshots older than 7 days
VACUUM ghn_analytics.orders;
VACUUM ghn_analytics.order_events;

-- [Local Trino 435 Docker Harness Equivalent]
-- ALTER TABLE iceberg.ghn_analytics.orders EXECUTE optimize;
-- ALTER TABLE iceberg.ghn_analytics.order_events EXECUTE optimize;
-- ALTER TABLE iceberg.ghn_analytics.orders EXECUTE expire_snapshots(retention_threshold => '7d');
-- ALTER TABLE iceberg.ghn_analytics.order_events EXECUTE expire_snapshots(retention_threshold => '7d');
