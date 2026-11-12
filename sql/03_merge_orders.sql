-- ============================================================================
-- Step 2: Pre-deduplication & Upsert into silver.orders (Current State)
-- Enforces Hierarchical State Machine with Partition Pruning
-- ============================================================================

MERGE INTO ghn_analytics.orders AS target
USING (
    SELECT 
        order_code,
        client_order_code,
        status,
        is_terminal,
        delivery_attempt,
        phase_rank,
        created_at,
        COALESCE(created_date, CAST(CAST(created_at AS DATE) AS VARCHAR)) AS created_date,
        updated_at,
        delivered_at,
        weight,
        shipping_fee,
        cod_amount,
        lead_time_hours,
        to_name,
        to_phone,
        to_address,
        to_province_id,
        to_district_id
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_code 
                ORDER BY 
                    is_terminal DESC, 
                    delivery_attempt DESC, 
                    phase_rank DESC, 
                    updated_at DESC
            ) AS rn
        FROM bronze_staging
    )
    WHERE rn = 1
) AS source
ON target.created_date = source.created_date 
   AND target.order_code = source.order_code
WHEN MATCHED AND (
    target.is_terminal = false AND (
        source.is_terminal = true OR
        source.delivery_attempt > target.delivery_attempt OR
        (source.delivery_attempt = target.delivery_attempt AND (
            source.phase_rank > target.phase_rank OR
            (source.phase_rank = target.phase_rank AND source.updated_at > target.updated_at)
        ))
    )
) THEN
    UPDATE SET 
        status = source.status,
        is_terminal = source.is_terminal,
        delivery_attempt = source.delivery_attempt,
        phase_rank = source.phase_rank,
        updated_at = source.updated_at,
        delivered_at = COALESCE(source.delivered_at, target.delivered_at),
        weight = COALESCE(source.weight, target.weight),
        shipping_fee = COALESCE(source.shipping_fee, target.shipping_fee),
        cod_amount = COALESCE(source.cod_amount, target.cod_amount),
        lead_time_hours = COALESCE(source.lead_time_hours, target.lead_time_hours),
        to_name = COALESCE(source.to_name, target.to_name),
        to_phone = COALESCE(source.to_phone, target.to_phone),
        to_address = COALESCE(source.to_address, target.to_address),
        to_province_id = COALESCE(source.to_province_id, target.to_province_id),
        to_district_id = COALESCE(source.to_district_id, target.to_district_id),
        ingested_at = current_timestamp
WHEN NOT MATCHED THEN
    INSERT (
        order_code, client_order_code, status, is_terminal, 
        delivery_attempt, phase_rank, created_at, created_date, 
        updated_at, delivered_at, weight, shipping_fee, 
        cod_amount, lead_time_hours, to_name, to_phone, 
        to_address, to_province_id, to_district_id, ingested_at
    )
    VALUES (
        source.order_code, source.client_order_code, source.status, source.is_terminal, 
        source.delivery_attempt, source.phase_rank, source.created_at, source.created_date, 
        source.updated_at, source.delivered_at, source.weight, source.shipping_fee, 
        source.cod_amount, source.lead_time_hours, source.to_name, source.to_phone, 
        source.to_address, source.to_province_id, source.to_district_id, current_timestamp
    );

