"""
Calcul de métriques sur le flux GPS.

À partir des positions successives d'un même véhicule, on dérive :
  - la distance parcourue depuis la position précédente (Haversine) ;
  - l'intervalle de temps écoulé ;
  - la vitesse calculée (distance / temps), en complément de la vitesse
    déclarée par le simulateur ;
  - un indicateur d'immobilité (véhicule quasi à l'arrêt).

Ces métriques comparent chaque ligne à la PRÉCÉDENTE du même véhicule.
En Spark, cela se fait avec une window function `lag` partitionnée par
vehicle_id et ordonnée par event_time.

ATTENTION : les window functions ordonnées (lag) ne sont PAS supportées
directement sur un flux streaming avec outputMode append sans agrégation.
Ce module est donc conçu pour être appliqué DANS un foreachBatch (sur un
DataFrame statique par micro-batch), pas sur le flux streaming continu.
C'est le pattern retenu pour l'enrichissement.
"""
import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# Rayon de la Terre en km (cohérent avec le générateur GPS du simulateur).
_EARTH_RADIUS_KM = 6371.0

# En dessous de cette vitesse (km/h), on considère le véhicule immobile.
_IMMOBILE_SPEED_THRESHOLD = 1.0


def _haversine_udf():
    """
    Construit une UDF Spark calculant la distance de Haversine (km) entre
    deux points (lat1, lon1) et (lat2, lon2).

    Retourne null si l'une des coordonnées est nulle (ex. première position
    d'un véhicule, sans précédent).
    """
    def haversine(lat1, lon1, lat2, lon2):
        if None in (lat1, lon1, lat2, lon2):
            return None
        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)
        a = (
            math.sin(d_lat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(d_lon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return _EARTH_RADIUS_KM * c

    return F.udf(haversine, DoubleType())


def add_movement_metrics(df: DataFrame) -> DataFrame:
    """
    Ajoute les métriques de déplacement à un DataFrame GPS (statique/batch).

    Colonnes ajoutées :
      - prev_lat, prev_lon, prev_event_time : position/temps précédents
        du même véhicule (null pour la première position) ;
      - distance_km        : distance depuis la position précédente ;
      - elapsed_seconds     : secondes écoulées depuis la précédente ;
      - computed_speed_kmh  : vitesse calculée (distance/temps) ;
      - is_immobile         : True si vitesse calculée quasi nulle.

    Nécessite les colonnes : vehicle_id, lat, lon, event_time.
    """
    # Fenêtre : pour chaque véhicule, on ordonne par temps pour accéder
    # à la position précédente via lag().
    w = Window.partitionBy("vehicle_id").orderBy("event_time")

    haversine = _haversine_udf()

    enriched = (
        df
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_event_time", F.lag("event_time").over(w))
        # Distance depuis la position précédente.
        .withColumn(
            "distance_km",
            haversine(F.col("prev_lat"), F.col("prev_lon"),
                      F.col("lat"), F.col("lon")),
        )
        # Temps écoulé en secondes (différence de timestamps).
        .withColumn(
            "elapsed_seconds",
            F.when(
                F.col("prev_event_time").isNotNull(),
                F.col("event_time").cast("long") - F.col("prev_event_time").cast("long"),
            ).otherwise(None),
        )
        # Vitesse calculée = distance / temps, convertie en km/h.
        # (distance_km / elapsed_seconds) * 3600
        .withColumn(
            "computed_speed_kmh",
            F.when(
                (F.col("elapsed_seconds").isNotNull()) & (F.col("elapsed_seconds") > 0),
                (F.col("distance_km") / F.col("elapsed_seconds")) * 3600.0,
            ).otherwise(None),
        )
        # Immobilité : vitesse calculée sous le seuil.
        .withColumn(
            "is_immobile",
            F.when(
                F.col("computed_speed_kmh").isNotNull(),
                F.col("computed_speed_kmh") < _IMMOBILE_SPEED_THRESHOLD,
            ).otherwise(None),
        )
    )

    return enriched