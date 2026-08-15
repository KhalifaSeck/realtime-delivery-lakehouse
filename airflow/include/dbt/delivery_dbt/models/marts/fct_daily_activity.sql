{{
  config(
    materialized='table'
  )
}}

-- ============================================================
-- Mart faits : fct_daily_activity
--
-- Agrégats journaliers de toute la flotte :
--   - nombre de véhicules actifs
--   - livraisons totales, réussies, échouées
--   - vitesse moyenne de la flotte
--
-- Utilisé par Power BI pour les tendances (courbes journalières).
-- ============================================================

WITH gps_daily AS (
    -- Métriques journalières basées sur les GPS
    SELECT
        DATE(event_time)                       AS activity_date,
        COUNT(DISTINCT vehicle_id)             AS n_active_vehicles,
        COUNT(DISTINCT driver_id)              AS n_active_drivers,
        ROUND(AVG(speed_kmh), 2)               AS fleet_avg_speed_kmh,
        COUNT(*)                               AS n_gps_points
    FROM {{ ref('stg_gps_events') }}
    GROUP BY DATE(event_time)
),

delivery_daily AS (
    -- Métriques journalières basées sur les livraisons
    SELECT
        DATE(event_time)                                                                       AS activity_date,
        COUNT(DISTINCT package_id)                                                             AS n_deliveries_touched,
        COUNT(DISTINCT CASE WHEN status = 'delivered' THEN package_id END)                     AS n_delivered,
        COUNT(DISTINCT CASE WHEN status = 'failed'    THEN package_id END)                     AS n_failed,
        COUNT(DISTINCT CASE WHEN status = 'picked_up' THEN package_id END)                     AS n_picked_up
    FROM {{ ref('stg_delivery_events') }}
    GROUP BY DATE(event_time)
)

SELECT
    COALESCE(g.activity_date, d.activity_date)      AS activity_date,
    COALESCE(g.n_active_vehicles, 0)                AS n_active_vehicles,
    COALESCE(g.n_active_drivers, 0)                 AS n_active_drivers,
    g.fleet_avg_speed_kmh,
    COALESCE(g.n_gps_points, 0)                     AS n_gps_points,
    COALESCE(d.n_deliveries_touched, 0)             AS n_deliveries_touched,
    COALESCE(d.n_delivered, 0)                      AS n_delivered,
    COALESCE(d.n_failed, 0)                         AS n_failed,
    COALESCE(d.n_picked_up, 0)                      AS n_picked_up,
    -- Taux de succès journalier
    CASE
        WHEN COALESCE(d.n_delivered, 0) + COALESCE(d.n_failed, 0) = 0 THEN NULL
        ELSE ROUND(100.0 * d.n_delivered / (d.n_delivered + d.n_failed), 2)
    END                                              AS daily_success_rate_pct,
    CURRENT_TIMESTAMP()                              AS mart_refreshed_at
FROM gps_daily g
FULL OUTER JOIN delivery_daily d ON g.activity_date = d.activity_date
ORDER BY activity_date DESC