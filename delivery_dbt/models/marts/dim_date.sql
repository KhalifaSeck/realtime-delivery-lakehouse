{{
  config(
    materialized='table'
  )
}}

-- ============================================================
-- Mart dimension : dim_date
--
-- Table calendrier générant une ligne par jour sur la période
-- couverte par les données (min et max des event_time).
--
-- Étend automatiquement la plage +/- 30 jours pour supporter
-- les filtres Power BI ("last 30 days" au-delà de la donnée).
-- ============================================================

WITH date_range AS (
    -- Détermine la plage à générer à partir des faits.
    SELECT
        DATEADD(day, -30, MIN(activity_date))    AS start_date,
        DATEADD(day, +30, MAX(activity_date))    AS end_date
    FROM {{ ref('fct_daily_activity') }}
),

date_spine AS (
    -- Génère une ligne par jour dans la plage via une séquence.
    -- Snowflake supporte GENERATOR pour créer des séries.
    SELECT
        DATEADD(day, seq4(), (SELECT start_date FROM date_range)) AS date_day
    FROM TABLE(GENERATOR(rowcount => 400))  -- 400 jours max, ajustable
    WHERE DATEADD(day, seq4(), (SELECT start_date FROM date_range))
        <= (SELECT end_date FROM date_range)
)

SELECT
    date_day,
    -- Extractions temporelles
    YEAR(date_day)                            AS year,
    QUARTER(date_day)                         AS quarter,
    MONTH(date_day)                           AS month,
    DAY(date_day)                             AS day_of_month,
    DAYOFWEEK(date_day)                       AS day_of_week_num,
    DAYNAME(date_day)                         AS day_of_week_name,
    MONTHNAME(date_day)                       AS month_name,
    WEEK(date_day)                            AS week_of_year,

    -- Flags calendrier
    CASE
        WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                       AS is_weekend,

    CASE
        WHEN DAYOFWEEK(date_day) IN (0, 6) THEN FALSE
        ELSE TRUE
    END                                       AS is_business_day,

    -- Positions relatives (utiles pour comparaisons)
    date_day - CURRENT_DATE()                 AS days_from_today,
    DATE_TRUNC('month', date_day)             AS month_start,
    LAST_DAY(date_day, 'month')               AS month_end,
    DATE_TRUNC('quarter', date_day)           AS quarter_start,
    DATE_TRUNC('year', date_day)              AS year_start,

    CURRENT_TIMESTAMP()                       AS mart_refreshed_at
FROM date_spine
ORDER BY date_day