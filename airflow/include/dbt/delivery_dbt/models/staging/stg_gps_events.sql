{{
  config(
    materialized='view'
  )
}}

-- ============================================================
-- Staging model : gps_events
--
-- Transformations :
--   1. Cast des types (VARCHAR -> types natifs)
--   2. Filtrage : lignes clairement invalides (vitesses négatives,
--      coordonnées hors Montréal)
--   3. Déduplication : garde la ligne la plus récemment chargée
--      pour chaque (vehicle_id, event_time)
-- ============================================================

WITH raw_gps AS (
    SELECT
        vehicle_id,
        driver_id,
        CAST(lat        AS FLOAT)         AS latitude,
        CAST(lon        AS FLOAT)         AS longitude,
        CAST(speed_kmh  AS FLOAT)         AS speed_kmh,
        CAST(event_time AS TIMESTAMP_TZ)  AS event_time,
        load_timestamp
    FROM {{ source('raw', 'gps_events') }}
    WHERE
        vehicle_id IS NOT NULL
        AND event_time IS NOT NULL
        AND speed_kmh >= 0
        AND lat BETWEEN 45.0 AND 46.5     -- bounding box Montréal
        AND lon BETWEEN -74.5 AND -73.0
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id, event_time
            ORDER BY load_timestamp DESC
        ) AS row_num
    FROM raw_gps
)

SELECT
    vehicle_id,
    driver_id,
    latitude,
    longitude,
    speed_kmh,
    event_time,
    load_timestamp
FROM deduplicated
WHERE row_num = 1