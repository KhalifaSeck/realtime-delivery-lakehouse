"""
Sink data lake : écrit l'historique brut des événements en Parquet.

Contrairement au sink Redis (état courant, une ligne par véhicule qu'on
écrase), ce sink conserve TOUT l'historique : chaque événement devient une
ligne Parquet immuable. C'est la couche "froide" analytique, destinée à
alimenter Snowflake puis dbt.

Partitionnement : par date d'événement (event_date) et par type d'événement
(event_type). Ça donne une arborescence du type :
    <lake>/events/event_date=2026-08-01/event_type=gps/part-*.parquet

En dev, <lake> est un chemin local (LAKE_OUTPUT_DIR, hors OneDrive).
En prod, ce sera un chemin ADLS Gen2 (abfss://...), sans changer la logique.

Écriture native Spark en streaming (format parquet), pas de foreachBatch :
Spark gère lui-même l'append incrémental et le partitionnement.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.config import LAKE_OUTPUT_DIR, checkpoint_path


def _prepare_for_lake(df: DataFrame, event_type: str) -> DataFrame:
    """
    Prépare un DataFrame d'événements pour l'écriture Parquet partitionnée.

    - Ajoute event_type (constante : 'gps', 'delivery', 'order', 'driver').
    - Ajoute event_date, dérivée de event_time (colonne de partition).

    event_time doit déjà exister (ajouté par le nettoyage). S'il est null
    pour une ligne, event_date le sera aussi : Spark la rangera dans une
    partition '__HIVE_DEFAULT_PARTITION__', ce qui reste acceptable.
    """
    return (
        df
        .withColumn("event_type", F.lit(event_type))
        .withColumn("event_date", F.to_date(F.col("event_time")))
    )


def write_to_lake(df: DataFrame, event_type: str, query_name: str):
    """
    Démarre l'écriture streaming d'un flux d'événements vers le data lake
    en Parquet, partitionné par event_date et event_type.

    - df         : DataFrame streaming nettoyé (avec event_time).
    - event_type : étiquette du type ('gps', 'delivery', ...).
    - query_name : nom unique de la requête (et de son checkpoint).

    Retourne la StreamingQuery démarrée.
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