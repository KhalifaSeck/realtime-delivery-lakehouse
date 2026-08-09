{{
  config(
    materialized='view'
  )
}}

-- ============================================================
-- Staging model : driver_events
--
-- Transformations :
--   1. Cast des types
--   2. Filtrage : driver_id non null, statuts valides
--   3. Déduplication : garde la ligne la plus récemment chargée
--      pour chaque (driver_id, event_time)
-- ============================================================

WITH raw_drivers AS (
    SELECT
        driver_id,
        vehicle_id,
        LOWER(TRIM(status))              AS status,
        CAST(event_time AS TIMESTAMP_TZ) AS event_time,
        load_timestamp
    FROM {{ source('raw', 'driver_events') }}
    WHERE
        driver_id IS NOT NULL
        AND event_time IS NOT NULL
        AND status IN ('online', 'offline', 'on_break', 'delivering')
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY driver_id, event_time
            ORDER BY load_timestamp DESC
        ) AS row_num
    FROM raw_drivers
)

SELECT
    driver_id,
    vehicle_id,
    status,
    event_time,
    load_timestamp
FROM deduplicated
WHERE row_num = 1