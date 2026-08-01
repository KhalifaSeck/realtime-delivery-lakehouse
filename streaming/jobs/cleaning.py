"""
Nettoyage et normalisation des événements validés.

Transformations appliquées :
  1. Conversion du timestamp (chaîne ISO 8601) en vrai TimestampType Spark
     -> colonne 'event_time', indispensable pour les fenêtres et les calculs
     de retard en aval.
  2. Normalisation des statuts (minuscules, sans espaces) pour delivery/driver.
  3. Filtrage des coordonnées hors des bornes plausibles de la région
     (garde-fou contre les valeurs GPS aberrantes).

Chaque fonction prend un DataFrame et retourne un DataFrame enrichi/filtré,
sans effet de bord : on chaîne les transformations proprement.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Bornes géographiques plausibles (large autour du Grand Montréal).
# Toute coordonnée hors de ces bornes est considérée aberrante.
_LAT_MIN, _LAT_MAX = 45.0, 46.5
_LON_MIN, _LON_MAX = -74.5, -73.0


def add_event_time(df: DataFrame) -> DataFrame:
    """
    Convertit la colonne 'timestamp' (chaîne ISO 8601) en TimestampType Spark,
    dans une nouvelle colonne 'event_time'.

    to_timestamp gère le format ISO 8601 (avec offset), retournant null si
    le parsing échoue — ce qui isolerait un timestamp corrompu sans planter.
    """
    return df.withColumn("event_time", F.to_timestamp(F.col("timestamp")))


def normalize_status(df: DataFrame) -> DataFrame:
    """
    Normalise la colonne 'status' si elle existe (delivery_events, orders,
    driver_events) : minuscules, espaces en bordure supprimés.

    Sans effet sur les GPS (pas de colonne status) : on vérifie sa présence.
    """
    if "status" not in df.columns:
        return df
    return df.withColumn("status", F.lower(F.trim(F.col("status"))))


def filter_valid_coordinates(df: DataFrame) -> DataFrame:
    """
    Filtre les lignes dont les coordonnées (lat/lon) sont hors des bornes
    plausibles de la région. Les lignes sans coordonnées (lat/lon null)
    sont CONSERVÉES : certains événements n'en ont pas légitimement
    (ex. une commande 'created' peut n'avoir que la destination).

    Ne s'applique que si les colonnes lat/lon existent.
    """
    if "lat" not in df.columns or "lon" not in df.columns:
        return df

    # Une coordonnée est acceptable si elle est nulle OU dans les bornes.
    lat_ok = F.col("lat").isNull() | (
        (F.col("lat") >= _LAT_MIN) & (F.col("lat") <= _LAT_MAX)
    )
    lon_ok = F.col("lon").isNull() | (
        (F.col("lon") >= _LON_MIN) & (F.col("lon") <= _LON_MAX)
    )
    return df.filter(lat_ok & lon_ok)


def clean_events(df: DataFrame) -> DataFrame:
    """
    Pipeline de nettoyage complet, à appliquer sur un flux d'événements validés.
    Enchaîne : event_time -> normalisation statut -> filtrage coordonnées.

    Retourne le DataFrame nettoyé, prêt pour l'enrichissement et les sinks.
    """
    return (
        df
        .transform(add_event_time)
        .transform(normalize_status)
        .transform(filter_valid_coordinates)
    )