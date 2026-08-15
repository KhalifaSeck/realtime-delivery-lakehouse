{{
  config(
    materialized='table'
  )
}}

-- ============================================================
-- Mart dimension : dim_vehicles
--
-- Une ligne par véhicule avec ses métriques d'activité GPS
-- agrégées sur toute la période disponible.
-- ============================================================

WITH daily_activity AS (
    -- Activité journalière par véhicule (agrégat intermédiaire).
    SELECT
        vehicle_id,
        DATE(event_time)              AS activity_date,
        COUNT(*)                      AS n_gps_points,
        AVG(speed_kmh)                AS avg_speed_kmh,
        MIN(speed_kmh)                AS min_speed_kmh,
        MAX(speed_kmh)                AS max_speed_kmh,
        COUNT(DISTINCT driver_id)     AS n_distinct_drivers
    FROM {{ ref('stg_gps_events') }}
    WHERE speed_kmh IS NOT NULL
    GROUP BY vehicle_id, DATE(event_time)
)

SELECT
    vehicle_id,
    COUNT(DISTINCT activity_date)         AS n_active_days,
    MIN(activity_date)                    AS first_active_date,
    MAX(activity_date)                    AS last_active_date,
    SUM(n_gps_points)                     AS total_gps_points,
    ROUND(AVG(avg_speed_kmh), 2)          AS avg_speed_kmh_overall,
    MAX(max_speed_kmh)                    AS max_speed_ever_kmh,
    MAX(n_distinct_drivers)               AS max_drivers_per_day,
    CURRENT_TIMESTAMP()                   AS mart_refreshed_at
FROM daily_activity
GROUP BY vehicle_id