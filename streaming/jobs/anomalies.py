"""
Détection d'anomalies sur le flux GPS enrichi de métriques.

S'appuie sur les colonnes produites par metrics.add_movement_metrics
(distance_km, elapsed_seconds, computed_speed_kmh, is_immobile) pour
repérer des situations problématiques :

  - SPEED_TOO_HIGH   : vitesse calculée aberrante (erreur GPS, saut de
                       position, ou données corrompues) ;
  - SPEED_MISMATCH   : gros écart entre vitesse déclarée et vitesse calculée ;
  - IMMOBILE         : véhicule à l'arrêt (déjà détecté par les métriques) ;
  - GPS_JUMP         : distance parcourue irréaliste sur un pas de temps court.

Chaque ligne reçoit :
  - is_anomaly    : booléen, au moins une anomalie détectée ;
  - anomaly_types : liste des types d'anomalie (array de chaînes).

Comme metrics.py, ce module est conçu pour un DataFrame statique
(dans foreachBatch), pas pour le flux streaming continu.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Seuils de détection (ajustables selon le contexte métier).
_MAX_PLAUSIBLE_SPEED_KMH = 130.0   # au-delà : aberrant pour livraison urbaine
_SPEED_MISMATCH_TOLERANCE = 50.0   # écart max toléré déclaré vs calculé (km/h)
_MAX_PLAUSIBLE_DISTANCE_KM = 2.0   # distance max plausible sur un pas (~quelques s)


def add_anomaly_flags(df: DataFrame) -> DataFrame:
    """
    Ajoute les indicateurs d'anomalie à un DataFrame GPS déjà enrichi
    de métriques de déplacement.

    Nécessite les colonnes : speed_kmh, computed_speed_kmh, distance_km,
    is_immobile (issues de add_movement_metrics).

    Ajoute :
      - anomaly_types : array des types d'anomalie détectés (vide si aucun) ;
      - is_anomaly    : True si au moins une anomalie.
    """
    # Chaque condition produit soit le nom de l'anomalie, soit null.
    # array() + filtrage des null = liste des anomalies présentes.

    speed_too_high = F.when(
        F.col("computed_speed_kmh") > _MAX_PLAUSIBLE_SPEED_KMH,
        F.lit("SPEED_TOO_HIGH"),
    )

    speed_mismatch = F.when(
        (F.col("computed_speed_kmh").isNotNull())
        & (F.col("speed_kmh").isNotNull())
        & (F.abs(F.col("computed_speed_kmh") - F.col("speed_kmh")) > _SPEED_MISMATCH_TOLERANCE),
        F.lit("SPEED_MISMATCH"),
    )

    immobile = F.when(
        F.col("is_immobile") == True,  # noqa: E712 (comparaison Spark, pas Python)
        F.lit("IMMOBILE"),
    )

    gps_jump = F.when(
        F.col("distance_km") > _MAX_PLAUSIBLE_DISTANCE_KM,
        F.lit("GPS_JUMP"),
    )

    # Construit un array des anomalies, puis retire les null.
    anomalies_array =  F.filter(
        F.array(speed_too_high, speed_mismatch, immobile, gps_jump),
        lambda x: x.isNotNull(),
    )

    return (
        df
        .withColumn("anomaly_types", anomalies_array)
        .withColumn("is_anomaly", F.size(F.col("anomaly_types")) > 0)
    )