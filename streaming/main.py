"""
Point d'entrée du job Spark Structured Streaming.

Pipeline GPS de bout en bout, avec DEUX sinks en parallèle :

    Kafka (gps_positions)
      -> lecture + parsing (read_kafka)
      -> validation (dead-letter écarté)
      -> nettoyage (event_time, coordonnées)
      -> [sink Redis]  état courant des véhicules (speed layer)
      -> [sink Lake]   historique Parquet partitionné (batch layer)

Les deux sinks consomment le MÊME DataFrame nettoyé mais écrivent vers des
destinations différentes, chacun avec son propre checkpoint isolé.

Lancement : python -m streaming.main
Prérequis : Kafka + Redis démarrés, simulateur en train de produire.
Ctrl+C pour arrêter (arrêt gracieux des deux requêtes).
"""
from streaming.config import get_spark, checkpoint_path
from streaming.jobs.read_kafka import read_topic_raw, parse_events
from streaming.jobs.validation import split_valid_invalid
from streaming.jobs.cleaning import clean_events
from streaming.sinks.redis_sink import write_gps_batch
from streaming.sinks.adls_sink import write_to_lake


def build_gps_stream(spark):
    """
    Construit le flux GPS nettoyé (sans le démarrer).
    Ce DataFrame sera consommé par les deux sinks.
    """
    raw = read_topic_raw(spark, "gps_positions")
    parsed = parse_events(raw, "gps_positions")
    valid, _invalid = split_valid_invalid(parsed, "gps_positions")
    cleaned = clean_events(valid)
    return cleaned


def run():
    spark = get_spark("DeliveryStreaming")
    spark.sparkContext.setLogLevel("WARN")

    gps_stream = build_gps_stream(spark)

    # --- Sink 1 : Redis (état courant, speed layer) ---
    gps_redis_query = (
        gps_stream.writeStream
        .foreachBatch(write_gps_batch)
        .option("checkpointLocation", checkpoint_path("gps_redis"))
        .trigger(processingTime="5 seconds")
        .queryName("gps_to_redis")
        .start()
    )

    # --- Sink 2 : Data lake (historique Parquet, batch layer) ---
    gps_lake_query = write_to_lake(gps_stream, event_type="gps", query_name="gps")

    print("=== Flux GPS démarré : Redis + Lake. Ctrl+C pour arrêter. ===")

    # Attend la fin de N'IMPORTE laquelle des requêtes (ou Ctrl+C).
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()