-- ============================================================================
-- Step 1: Idempotent Merge into silver.order_events
-- Pre-deduplicates source records by event_id and appends if not exists
-- ============================================================================

MERGE INTO ghn_analytics.order_events AS target
USING (
    SELECT 
        order_code,
        status,
        updated_at,
        created_date,
        delivery_attempt,
        phase_rank,
        to_hex(sha256(to_utf8(concat(order_code, '||', status, '||', to_iso8601(updated_at))))) AS event_id,
        ROW_NUMBER() OVER (
            PARTITION BY order_code, status, updated_at 
            ORDER BY updated_at DESC
        ) AS rn
    FROM bronze_staging
) AS source
ON target.created_date = source.created_date 
   AND target.event_id = source.event_id
WHEN NOT MATCHED AND source.rn = 1 THEN
    INSERT (
        order_code, 
        status, 
        updated_at, 
        created_date, 
        event_id, 
        delivery_attempt,
        phase_rank,
        ingested_at
    )
    VALUES (
        source.order_code, 
        source.status, 
        source.updated_at, 
        source.created_date, 
        source.event_id, 
        source.delivery_attempt,
        source.phase_rank,
        current_timestamp
    );

