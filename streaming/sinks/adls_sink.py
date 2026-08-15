"""
Sink data lake : écrit l'historique enrichi des événements en Parquet.

Deux modes d'écriture selon le besoin :
  - write_to_lake        : écriture Parquet native (flux brut, sans window).
  - write_enriched_to_lake : écriture via foreachBatch, avec enrichissement
                             (métriques + anomalies) appliqué à chaque batch.

Dual-write : écrit simultanément en local ET dans ADLS Gen2 (contrôlé
par les flags SPARK_WRITE_LOCAL et SPARK_WRITE_ADLS de config.py).

Partitionnement : event_date / event_type.
"""
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.config import (
    LAKE_OUTPUT_DIR,
    checkpoint_path,
    ADLS_BASE_URI,
    SPARK_WRITE_ADLS,
    adls_output_path,
    adls_checkpoint_path,
)
from streaming.jobs.enrichment import enrich_gps


# Flag pour écrire aussi en local (défaut : True pour rester débuggable)
SPARK_WRITE_LOCAL = os.getenv("SPARK_WRITE_LOCAL", "true").lower() == "true"


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


def _local_output_path(event_type: str) -> str:
    """Chemin de sortie local (Windows)."""
    return f"{LAKE_OUTPUT_DIR}\\events\\{event_type}"


# ============================================================
# Mode 1 : écriture native Parquet (sans enrichissement)
# ============================================================
def write_to_lake(df: DataFrame, event_type: str, query_name: str):
    """
    Écriture Parquet native (flux brut sans enrichissement).
    Dual-write : local + ADLS selon les flags SPARK_WRITE_LOCAL / SPARK_WRITE_ADLS.

    Retourne la liste des StreamingQuery démarrées (1 ou 2 selon la config).
    """
    prepared = _prepare_for_lake(df, event_type)
    queries = []

    # Sink LOCAL
    if SPARK_WRITE_LOCAL:
        q_local = (
            prepared.writeStream
            .format("parquet")
            .option("path", _local_output_path(event_type))
            .option("checkpointLocation", checkpoint_path(f"lake_{query_name}"))
            .partitionBy("event_date", "event_type")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .queryName(query_name)
            .start()
        )
        queries.append(q_local)

    # Sink ADLS
    if SPARK_WRITE_ADLS:
        q_adls = (
            prepared.writeStream
            .format("parquet")
            .option("path", adls_output_path(event_type))
            .option("checkpointLocation", adls_checkpoint_path(query_name))
            .partitionBy("event_date", "event_type")
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .queryName(f"{query_name}_adls")
            .start()
        )
        queries.append(q_adls)

    return queries


# ============================================================
# Mode 2 : écriture avec enrichissement (via foreachBatch)
# ============================================================
def write_enriched_to_lake(df: DataFrame, event_type: str, query_name: str):
    """
    Écriture Parquet AVEC enrichissement (métriques + anomalies), via
    foreachBatch. Utilisé pour le flux GPS.

    Dual-write intégré dans le foreachBatch : chaque batch enrichi est
    écrit sur local ET/OU ADLS selon les flags.
    """
    local_path = _local_output_path(event_type)
    adls_path = adls_output_path(event_type)

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Enrichissement (window functions OK sur DF statique)
        enriched = enrich_gps(batch_df)
        prepared = _prepare_for_lake(enriched, event_type)

        # Cache le DF pour éviter de recalculer si double écriture.
        if SPARK_WRITE_LOCAL and SPARK_WRITE_ADLS:
            prepared.persist()

        # Écriture LOCAL
        if SPARK_WRITE_LOCAL:
            (
                prepared.write
                .format("parquet")
                .partitionBy("event_date", "event_type")
                .mode("append")
                .save(local_path)
            )

        # Écriture ADLS
        if SPARK_WRITE_ADLS:
            (
                prepared.write
                .format("parquet")
                .partitionBy("event_date", "event_type")
                .mode("append")
                .save(adls_path)
            )

        if SPARK_WRITE_LOCAL and SPARK_WRITE_ADLS:
            prepared.unpersist()

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path(f"lake_{query_name}"))
        .trigger(processingTime="10 seconds")
        .queryName(query_name)
        .start()
    )