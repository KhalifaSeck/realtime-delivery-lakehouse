{{
  config(
    materialized='table'
  )
}}

-- ============================================================
-- Mart faits : fct_deliveries
--
-- Une ligne par package_id avec le cycle de livraison complet :
--   - horodatage de chaque étape (picked_up, in_transit, delivered/failed)
--   - durées entre étapes
--   - statut final
--   - chauffeur résolu par ASOF JOIN sur stg_driver_events
--
-- Architecture : consomme directement STAGING (pas de couche intermediate).
-- Le temporal join package -> driver est intégré ici plutôt que d'être
-- externalisé, car il n'est utilisé que par ce fait.
-- ============================================================

WITH deliveries AS (
    SELECT
        package_id,
        order_id,
        vehicle_id,
        status         AS delivery_status,
        event_time     AS delivery_event_time
    FROM {{ ref('stg_delivery_events') }}
),

orders AS (
    SELECT
        order_id,
        event_time     AS order_created_at
    FROM {{ ref('stg_orders') }}
),

-- Historique des assignations vehicle_id -> driver_id (pour ASOF JOIN)
driver_history AS (
    SELECT
        driver_id,
        vehicle_id,
        event_time     AS driver_assigned_at
    FROM {{ ref('stg_driver_events') }}
    WHERE vehicle_id IS NOT NULL
      AND driver_id IS NOT NULL
),

-- ASOF JOIN : pour chaque event de livraison, trouver le chauffeur
-- au volant du véhicule à ce moment-là (dernier driver_event antérieur).
deliveries_with_driver AS (
    SELECT
        d.package_id,
        d.order_id,
        d.vehicle_id,
        d.delivery_status,
        d.delivery_event_time,
        dh.driver_id
    FROM deliveries d
    ASOF JOIN driver_history dh
        MATCH_CONDITION (d.delivery_event_time >= dh.driver_assigned_at)
        ON d.vehicle_id = dh.vehicle_id
),

-- Pivot : une ligne par package avec les timestamps de chaque statut
pivoted AS (
    SELECT
        package_id,
        MAX(order_id)                                                                  AS order_id,
        MAX(vehicle_id)                                                                AS vehicle_id,
        MAX(driver_id)                                                                 AS driver_id,
        MAX(CASE WHEN delivery_status = 'picked_up'  THEN delivery_event_time END)     AS picked_up_at,
        MAX(CASE WHEN delivery_status = 'in_transit' THEN delivery_event_time END)     AS in_transit_at,
        MAX(CASE WHEN delivery_status = 'delivered'  THEN delivery_event_time END)     AS delivered_at,
        MAX(CASE WHEN delivery_status = 'failed'     THEN delivery_event_time END)     AS failed_at
    FROM deliveries_with_driver
    GROUP BY package_id
)

SELECT
    p.package_id,
    p.order_id,
    p.vehicle_id,
    p.driver_id,
    o.order_created_at,
    p.picked_up_at,
    p.in_transit_at,
    p.delivered_at,
    p.failed_at,
    -- Statut final (priorité : delivered > failed > in_transit > picked_up > pending)
    CASE
        WHEN p.delivered_at   IS NOT NULL THEN 'delivered'
        WHEN p.failed_at      IS NOT NULL THEN 'failed'
        WHEN p.in_transit_at  IS NOT NULL THEN 'in_transit'
        WHEN p.picked_up_at   IS NOT NULL THEN 'picked_up'
        ELSE 'pending'
    END AS final_status,
    -- Métriques temporelles (minutes)
    DATEDIFF('minute', o.order_created_at, p.picked_up_at)   AS minutes_order_to_pickup,
    DATEDIFF('minute', p.picked_up_at,     p.delivered_at)   AS minutes_pickup_to_delivery,
    DATEDIFF('minute', o.order_created_at, p.delivered_at)   AS minutes_order_to_delivery,
    -- Flags booléens
    CASE WHEN p.delivered_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_delivered,
    CASE WHEN p.failed_at    IS NOT NULL THEN TRUE ELSE FALSE END AS is_failed,
    CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM pivoted p
LEFT JOIN orders o ON p.order_id = o.order_id