-- ============================================================
-- Worksheet delivery lakehouse
--
-- Requêtes SQL utiles pour explorer, valider et administrer
-- le projet delivery-lakehouse dans Snowflake.
--
-- Usage :
--   1. Ouvre l'interface Snowflake
--   2. Crée une nouvelle worksheet (bouton "+ Worksheet")
--   3. Colle ce contenu
--   4. Sauvegarde-la sous le nom "delivery_lakehouse"
-- ============================================================

-- ========== Contexte : bascule sur le projet ==========
USE ROLE DELIVERY_ROLE;
USE WAREHOUSE DELIVERY_WH;
USE DATABASE DELIVERY_LAKEHOUSE;
USE SCHEMA RAW;

-- ========== Vérifier les tables créées par Terraform ==========
SHOW TABLES IN SCHEMA DELIVERY_LAKEHOUSE.RAW;

-- Détails des colonnes de la table GPS_EVENTS
DESCRIBE TABLE GPS_EVENTS;

-- ========== Comptages par type d'événement ==========
SELECT 'GPS_EVENTS' AS event_type, COUNT(*) AS n_rows FROM GPS_EVENTS
UNION ALL
SELECT 'DELIVERY_EVENTS', COUNT(*) FROM DELIVERY_EVENTS
UNION ALL
SELECT 'ORDERS', COUNT(*) FROM ORDERS
UNION ALL
SELECT 'DRIVER_EVENTS', COUNT(*) FROM DRIVER_EVENTS
ORDER BY event_type;

-- ========== Contrôles qualité rapides ==========
-- Événements par jour (fraîcheur)
SELECT
    DATE(EVENT_TIME) AS event_date,
    COUNT(*) AS n_events
FROM GPS_EVENTS
GROUP BY event_date
ORDER BY event_date DESC
LIMIT 7;

-- Statuts de livraison observés
SELECT STATUS, COUNT(*) AS n
FROM DELIVERY_EVENTS
GROUP BY STATUS
ORDER BY n DESC;

-- Chauffeurs actifs (dernier statut par driver_id)
WITH latest_status AS (
    SELECT
        DRIVER_ID,
        STATUS,
        EVENT_TIME,
        ROW_NUMBER() OVER (
            PARTITION BY DRIVER_ID
            ORDER BY EVENT_TIME DESC
        ) AS rn
    FROM DRIVER_EVENTS
)
SELECT STATUS, COUNT(*) AS n_drivers
FROM latest_status
WHERE rn = 1
GROUP BY STATUS
ORDER BY n_drivers DESC;

-- ========== Audit d'ingestion ==========
-- Combien de lignes ingérées par jour (via LOAD_TIMESTAMP)
SELECT
    DATE(LOAD_TIMESTAMP) AS load_date,
    COUNT(*) AS n_rows_loaded
FROM GPS_EVENTS
GROUP BY load_date
ORDER BY load_date DESC;