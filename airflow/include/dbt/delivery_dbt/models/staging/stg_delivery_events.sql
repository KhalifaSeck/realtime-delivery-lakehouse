{{
  config(
    materialized='view'
  )
}}

-- ============================================================
-- Staging model : delivery_events
--
-- Transformations :
--   1. Cast des types
--   2. Filtrage : IDs non nuls, statuts valides
--   3. Déduplication : garde la ligne la plus récemment chargée
--      pour chaque (package_id, event_time)
-- ============================================================

WITH raw_delivery AS (
    SELECT
        package_id,
        order_id,
        vehicle_id,
        driver_id,
        LOWER(TRIM(status))              AS status,
        CAST(event_time AS TIMESTAMP_TZ) AS event_time,
        load_timestamp
    FROM {{ source('raw', 'delivery_events') }}
    WHERE
        package_id IS NOT NULL
        AND order_id IS NOT NULL
        AND event_time IS NOT NULL
        AND status IN ('picked_up', 'in_transit', 'delivered', 'failed')
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY package_id, event_time
            ORDER BY load_timestamp DESC
        ) AS row_num
    FROM raw_delivery
)

SELECT
    package_id,
    order_id,
    vehicle_id,
    driver_id,
    status,
    event_time,
    load_timestamp
FROM deduplicated
WHERE row_num = 1