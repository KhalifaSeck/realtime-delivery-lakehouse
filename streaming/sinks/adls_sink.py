"""
Sink data lake : écrit l'historique enrichi des événements en Parquet.

Deux modes d'écriture selon le besoin :
  - write_to_lake        : écriture Parquet native (flux brut, sans window).
  - write_enriched_to_lake : écriture via foreachBatch, avec enrichissement
                             (métriques + anomalies) appliqué à chaque batch.
                             Nécessaire car l'enrichissement utilise des
                             window functions incompatibles avec le streaming
                             natif append.

Partitionnement : event_date / event_type, comme avant.
En dev : chemin local (LAKE_OUTPUT_DIR). En prod : ADLS Gen2.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.config import LAKE_OUTPUT_DIR, checkpoint_path
from streaming.jobs.enrichment import enrich_gps


def _prepare_for_lake(df: DataFrame, event_type: str) -> DataFrame:
    """
    Ajoute les colonnes de partition (event_type, event_date).
    event_time doit déjà exister (ajouté par le nettoyage).
    """
    return (
        df
        .withColumn("event_type", F.lit(event_type))
        .withColumn("event_date", F.to_date(F.col("event_time")))
    )


def write_to_lake(df: DataFrame, event_type: str, query_name: str):
    """
    Écriture Parquet native (flux brut sans enrichissement).
    Conservée pour les topics qui n'ont pas besoin de window functions
    (delivery, orders, driver events aux étapes suivantes).
    """
    prepared = _prepare_for_lake(df, event_type)
    output_path = f"{LAKE_OUTPUT_DIR}\\events"

    return (
        prepared.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path(f"lake_{query_name}"))
        .partitionBy("event_date", "event_type")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName(query_name)
        .start()
    )


def write_enriched_to_lake(df: DataFrame, event_type: str, query_name: str):
    """
    Écriture Parquet AVEC enrichissement (métriques + anomalies), via
    foreachBatch. Utilisé pour le flux GPS, qui bénéficie des window
    functions (vitesse, distance, immobilité, anomalies).

    Chaque micro-batch est enrichi (DataFrame statique) puis écrit en
    Parquet partitionné, en mode append.
    """
    output_path = f"{LAKE_OUTPUT_DIR}\\events"

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        # Enrichissement sur le batch statique (window functions OK ici).
        enriched = enrich_gps(batch_df)
        prepared = _prepare_for_lake(enriched, event_type)
        # anomaly_types est un array -> Parquet le gère nativement.
        (
            prepared.write
            .format("parquet")
            .partitionBy("event_date", "event_type")
            .mode("append")
            .save(output_path)
        )

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path(f"lake_{query_name}"))
        .trigger(processingTime="10 seconds")
        .queryName(query_name)
        .start()
    )