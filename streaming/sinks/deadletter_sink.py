"""
Sink dead-letter : écrit les événements invalides (rejetés à la validation).

Les événements qui échouent la validation (clés obligatoires manquantes,
JSON mal parsé) ne sont pas jetés : ils sont historisés en Parquet dans
un dossier dédié, pour inspection et pour la métrique "DLQ size".

Écriture Parquet native, partitionnée par date de rejet et topic source.
Arborescence :
    <lake>/dead_letter/reject_date=YYYY-MM-DD/reject_topic=<topic>/*.parquet

Contrairement au lake principal (événements valides enrichis), la DLQ
conserve les lignes TELLES QUELLES au moment du rejet, plus les colonnes
d'annotation (reject_reason, reject_topic) ajoutées par la validation.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.config import LAKE_OUTPUT_DIR, checkpoint_path


def write_dead_letter(df: DataFrame, query_name: str):
    """
    Démarre l'écriture streaming des événements invalides vers la DLQ.

    - df : DataFrame streaming des invalides (avec reject_reason, reject_topic).
    - query_name : nom unique de la requête et de son checkpoint.

    Ajoute une colonne reject_date (date du jour de traitement) pour le
    partitionnement, puis écrit en Parquet append partitionné.
    """
    output_path = f"{LAKE_OUTPUT_DIR}\\dead_letter"

    # Date de rejet = maintenant (moment du traitement). current_date()
    # est évalué à chaque micro-batch.
    prepared = df.withColumn("reject_date", F.current_date())

    return (
        prepared.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path(f"dlq_{query_name}"))
        .partitionBy("reject_date", "reject_topic")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName(query_name)
        .start()
    )