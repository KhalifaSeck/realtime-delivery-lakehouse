{{
  config(
    materialized='table'
  )
}}

-- ============================================================
-- Mart dimension : dim_drivers
-- ============================================================

WITH latest_status AS (
    SELECT
        driver_id,
        vehicle_id,
        status         AS current_status,
        event_time     AS last_status_change,
        ROW_NUMBER() OVER (
            PARTITION BY driver_id
            ORDER BY event_time DESC
        ) AS row_num
    FROM {{ ref('stg_driver_events') }}
),

driver_activity AS (
    SELECT
        driver_id,
        COUNT(*)                    AS total_status_changes,
        MIN(event_time)             AS first_seen_at,
        MAX(event_time)             AS last_seen_at,
        DATEDIFF('hour', MIN(event_time), MAX(event_time)) AS active_hours
    FROM {{ ref('stg_driver_events') }}
    GROUP BY driver_id
),

delivery_counts AS (
    SELECT
        driver_id,
        COUNT(DISTINCT CASE WHEN status = 'delivered' THEN package_id END) AS n_deliveries_completed,
        COUNT(DISTINCT CASE WHEN status = 'failed'    THEN package_id END) AS n_deliveries_failed
    FROM {{ ref('stg_delivery_events') }}
    WHERE driver_id IS NOT NULL
    GROUP BY driver_id
)

SELECT
    ls.driver_id,
    ls.vehicle_id                              AS current_vehicle_id,
    ls.current_status,
    ls.last_status_change,
    da.first_seen_at,
    da.last_seen_at,
    da.active_hours,
    da.total_status_changes,
    COALESCE(dc.n_deliveries_completed, 0)     AS n_deliveries_completed,
    COALESCE(dc.n_deliveries_failed, 0)        AS n_deliveries_failed,
    CASE
        WHEN COALESCE(dc.n_deliveries_completed, 0) + COALESCE(dc.n_deliveries_failed, 0) = 0
        THEN NULL
        ELSE ROUND(
            100.0 * dc.n_deliveries_completed
            / (dc.n_deliveries_completed + dc.n_deliveries_failed),
            2
        )
    END                                        AS success_rate_pct,
    CURRENT_TIMESTAMP()                        AS mart_refreshed_at
FROM latest_status ls
LEFT JOIN driver_activity da ON ls.driver_id = da.driver_id
LEFT JOIN delivery_counts dc ON ls.driver_id = dc.driver_id
WHERE ls.row_num = 1