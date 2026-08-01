"""
Point d'entrée du job Spark Structured Streaming.

Assemble le pipeline GPS de bout en bout :
    Kafka (gps_positions)
      -> lecture + parsing (read_kafka)
      -> validation (dead-letter écarté)
      -> nettoyage (event_time, coordonnées)
      -> sink Redis (état courant des véhicules)

Ce fichier grossira aux étapes suivantes (autres topics, sink ADLS,
enrichissement). Pour l'instant : le flux GPS -> Redis, en continu.

Lancement : python -m streaming.main
Prérequis : Kafka + Redis démarrés, simulateur en train de produire.
Ctrl+C pour arrêter.
"""
from streaming.config import get_spark, checkpoint_path
from streaming.jobs.read_kafka import read_topic_raw, parse_events
from streaming.jobs.validation import split_valid_invalid
from streaming.jobs.cleaning import clean_events
from streaming.sinks.redis_sink import write_gps_batch


def build_gps_stream(spark):
    """
    Construit le flux GPS nettoyé (sans le démarrer).
    Retourne un DataFrame streaming prêt à être écrit vers Redis.
    """
    raw = read_topic_raw(spark, "gps_positions")
    parsed = parse_events(raw, "gps_positions")

    # On ne garde que les événements valides (le dead-letter est écarté ici ;
    # on le branchera vers un log/stockage à une étape ultérieure).
    valid, _invalid = split_valid_invalid(parsed, "gps_positions")

    cleaned = clean_events(valid)
    return cleaned


def run():
    spark = get_spark("DeliveryStreaming")
    spark.sparkContext.setLogLevel("WARN")

    gps_stream = build_gps_stream(spark)

    # Écriture vers Redis via foreachBatch.
    gps_query = (
        gps_stream.writeStream
        .foreachBatch(write_gps_batch)
        .option("checkpointLocation", checkpoint_path("gps_redis"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("=== Flux GPS -> Redis démarré. Ctrl+C pour arrêter. ===")
    gps_query.awaitTermination()


if __name__ == "__main__":
    run()