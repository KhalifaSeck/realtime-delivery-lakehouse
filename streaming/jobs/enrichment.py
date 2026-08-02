"""
Enrichissement du flux GPS : métriques de déplacement + anomalies.

Orchestre, dans l'ordre :
    clean_events (déjà fait en amont)
      -> add_movement_metrics (vitesse, distance, immobilité)
      -> add_anomaly_flags    (types d'anomalie, is_anomaly)

IMPORTANT : cet enrichissement repose sur des window functions (lag)
non supportées sur un flux streaming append. Il doit être appliqué
DANS un foreachBatch, sur le DataFrame statique de chaque micro-batch.
La fonction enrich_batch est conçue pour cet usage.
"""
from pyspark.sql import DataFrame

from streaming.jobs.metrics import add_movement_metrics
from streaming.jobs.anomalies import add_anomaly_flags


def enrich_gps(df: DataFrame) -> DataFrame:
    """
    Applique l'enrichissement complet à un DataFrame GPS statique
    (déjà validé et nettoyé) : métriques puis anomalies.

    Retourne le DataFrame enrichi de toutes les colonnes dérivées.
    """
    return (
        df
        .transform(add_movement_metrics)
        .transform(add_anomaly_flags)
    )