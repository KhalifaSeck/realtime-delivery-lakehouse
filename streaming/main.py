"""
Point d'entrée du job Spark Structured Streaming.

Traite les 4 topics en parallèle (DLQ temporairement suspendues) :

    gps_positions   -> Redis (état courant) + lake enrichi
    delivery_events -> lake brut
    orders          -> lake brut
    driver_events   -> lake brut

Chaque requête a son checkpoint isolé. Scheduler FAIR activé (config.py)
pour partager équitablement les ressources entre les requêtes.

Lancement : python -m streaming.main
Ctrl+C pour arrêter.
"""
import time

from streaming.config import get_spark, checkpoint_path
from streaming.jobs.pipeline import build_topic_pipeline
from streaming.sinks.redis_sink import write_gps_batch
from streaming.sinks.adls_sink import write_to_lake, write_enriched_to_lake
from streaming.sinks.deadletter_sink import write_dead_letter


def run():
    spark = get_spark("DeliveryStreaming")
    spark.sparkContext.setLogLevel("WARN")

    # ============================================================
    # GPS : Redis (état courant) + lake enrichi
    # ============================================================
    gps_valid, gps_invalid = build_topic_pipeline(spark, "gps_positions")

    gps_valid.writeStream \
        .foreachBatch(write_gps_batch) \
        .option("checkpointLocation", checkpoint_path("gps_redis")) \
        .trigger(processingTime="5 seconds") \
        .queryName("gps_to_redis") \
        .start()

    write_enriched_to_lake(gps_valid, event_type="gps", query_name="lake_gps")

    # ============================================================
    # delivery_events / orders / driver_events : lake brut
    # ============================================================
    delivery_valid, delivery_invalid = build_topic_pipeline(spark, "delivery_events")
    write_to_lake(delivery_valid, event_type="delivery", query_name="lake_delivery")

    orders_valid, orders_invalid = build_topic_pipeline(spark, "orders")
    write_to_lake(orders_valid, event_type="order", query_name="lake_orders")

    driver_valid, driver_invalid = build_topic_pipeline(spark, "driver_events")
    write_to_lake(driver_valid, event_type="driver", query_name="lake_drivers")

    # ============================================================
    # DLQ suspendues temporairement (simulateur ne produit que du valide).
    # À réactiver après validation des 4 flux lake.
    # ============================================================
    # write_dead_letter(gps_invalid, query_name="dlq_gps")
    # write_dead_letter(delivery_invalid, query_name="dlq_delivery")
    # write_dead_letter(orders_invalid, query_name="dlq_orders")
    # write_dead_letter(driver_invalid, query_name="dlq_drivers")

    print("=== 5 flux démarrés (DLQ suspendues). Ctrl+C pour arrêter. ===")

    # Monitoring : affiche l'activité de chaque requête toutes les 30s.
    try:
        while True:
            time.sleep(30)
            print("\n--- État des requêtes ---")
            for q in spark.streams.active:
                progress = q.lastProgress
                if progress:
                    rows = progress.get("numInputRows", 0)
                    print(f"  {q.name:16} | batch {progress.get('batchId', '?'):>3} | {rows:>5} lignes | statut: {q.status['message'][:50]}")
                else:
                    print(f"  {q.name:16} | (aucun batch encore) | statut: {q.status['message'][:50]}")
    except KeyboardInterrupt:
        print("\nArrêt demandé...")
        for q in spark.streams.active:
            q.stop()


if __name__ == "__main__":
    run()