-- ============================================================================
-- Step 4: Security & Privacy Views (Law on Personal Data Protection 91/2025/QH15)
-- Masks sensitive PII for Business Intelligence and data analysts while
-- preserving 'REDACTED' markers after Right-to-be-Forgotten execution.
-- ============================================================================

CREATE OR REPLACE VIEW ghn_analytics.orders_masked AS
SELECT 
    order_code,
    client_order_code,
    status,
    is_terminal,
    delivery_attempt,
    phase_rank,
    created_at,
    created_date,
    updated_at,
    delivered_at,
    weight,
    shipping_fee,
    cod_amount,
    lead_time_hours,
    CASE
        WHEN to_name IS NULL OR to_name = 'REDACTED' THEN to_name
        ELSE CONCAT(SUBSTR(to_name, 1, 1), '***')
    END AS to_name_masked,
    CASE
        WHEN to_phone IS NULL OR to_phone = 'REDACTED' THEN to_phone
        ELSE CONCAT(SUBSTR(to_phone, 1, 3), '*****', SUBSTR(to_phone, 9, 2))
    END AS to_phone_masked,
    to_province_id,
    to_district_id,
    ingested_at
FROM ghn_analytics.orders;
