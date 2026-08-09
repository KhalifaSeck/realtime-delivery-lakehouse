{{
  config(
    materialized='view'
  )
}}

-- ============================================================
-- Staging model : orders
--
-- Transformations :
--   1. Cast des types
--   2. Filtrage : IDs non nuls
--   3. Déduplication : un order_id unique
-- ============================================================

WITH raw_orders AS (
    SELECT
        order_id,
        package_id,
        LOWER(TRIM(status))              AS status,
        CAST(event_time AS TIMESTAMP_TZ) AS event_time,
        load_timestamp
    FROM {{ source('raw', 'orders') }}
    WHERE
        order_id IS NOT NULL
        AND package_id IS NOT NULL
        AND event_time IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY load_timestamp DESC, event_time DESC
        ) AS row_num
    FROM raw_orders
)

SELECT
    order_id,
    package_id,
    status,
    event_time,
    load_timestamp
FROM deduplicated
WHERE row_num = 1