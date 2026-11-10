-- ============================================================================
-- GHN Logistics Analytics Platform: Iceberg Tables DDL (Athena v3 / Trino Engine)
-- Target Catalog: Glue Data Catalog (AWS) / MinIO REST Catalog (Local)
-- Database: ghn_analytics
-- ============================================================================

CREATE DATABASE IF NOT EXISTS ghn_analytics
COMMENT 'GHN Logistics Data Lakehouse - Silver and Gold layers';

-- ----------------------------------------------------------------------------
-- 1. Table: silver.order_events (Append-only Event Log)
-- Source of Truth for State Transition History and Lead-time KPI computation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ghn_analytics.order_events (
    order_code VARCHAR,
    status VARCHAR,
    updated_at TIMESTAMP(6) WITH TIME ZONE,
    created_date VARCHAR,
    event_id VARCHAR,
    delivery_attempt INT,
    phase_rank INT,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    table_type = 'ICEBERG',
    format = 'PARQUET',
    partitioning = ARRAY['created_date'],
    is_external = false
);

-- ----------------------------------------------------------------------------
-- 2. Table: silver.orders (Current State Snapshot)
-- Source of Truth for Current Order States, updated via MERGE INTO
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ghn_analytics.orders (
    order_code VARCHAR,
    client_order_code VARCHAR,
    status VARCHAR,
    is_terminal BOOLEAN,
    delivery_attempt INT,
    phase_rank INT,
    created_at TIMESTAMP(6) WITH TIME ZONE,
    created_date VARCHAR,
    updated_at TIMESTAMP(6) WITH TIME ZONE,
    delivered_at TIMESTAMP(6) WITH TIME ZONE,
    weight DECIMAL(10, 2),
    shipping_fee DECIMAL(12, 2),
    cod_amount DECIMAL(12, 2),
    lead_time_hours DECIMAL(8, 2),
    to_name VARCHAR,
    to_phone VARCHAR,
    to_address VARCHAR,
    to_province_id INT,
    to_district_id INT,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    table_type = 'ICEBERG',
    format = 'PARQUET',
    partitioning = ARRAY['created_date', 'bucket(order_code, 16)'],
    is_external = false
);

-- ----------------------------------------------------------------------------
-- 3. Table: gold.delivery_kpis (Daily Aggregated Logistics Metrics)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ghn_analytics.delivery_kpis (
    created_date VARCHAR,
    from_province_id INT,
    to_province_id INT,
    total_orders INT,
    delivered_orders INT,
    first_attempt_delivered INT,
    first_attempt_completed INT,
    returned_orders INT,
    mature_cohort_orders INT,
    avg_lead_time_hours DOUBLE,
    fadr_pct DOUBLE,
    return_rate_pct DOUBLE,
    aggregated_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    table_type = 'ICEBERG',
    format = 'PARQUET',
    partitioning = ARRAY['created_date'],
    is_external = false
);

