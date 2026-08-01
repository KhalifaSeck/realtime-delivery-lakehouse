"""
Lecture des topics Kafka en streaming.

Fournit deux fonctions :
  - read_topic_raw : ouvre un flux Kafka brut (clé/valeur en bytes).
  - parse_events   : décode la valeur JSON selon le schéma du topic,
                     extrait la clé Kafka et le timestamp Kafka.

Le résultat est un DataFrame streaming structuré, prêt pour la validation
et le nettoyage (étapes suivantes).
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from streaming.config import KAFKA_SERVERS
from streaming.schemas import SCHEMA_BY_TOPIC


def read_topic_raw(spark: SparkSession, topic: str) -> DataFrame:
    """
    Ouvre un flux de lecture Kafka pour un topic donné.

    Retourne le DataFrame Kafka brut, avec les colonnes standard :
    key, value (bytes), topic, partition, offset, timestamp.

    - startingOffsets 'latest' : on ne lit que les nouveaux messages
      (pas tout l'historique du topic). Pratique en dev pour repartir propre.
    - failOnDataLoss 'false' : ne fait pas planter le flux si des offsets
      ont disparu (rétention Kafka), utile en développement.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df: DataFrame, topic: str) -> DataFrame:
    """
    Transforme un DataFrame Kafka brut en événements structurés.

    - Décode la clé Kafka (bytes -> string) : c'est notre clé de partition
      métier (vehicle_id / package_id / driver_id selon le topic).
    - Décode la valeur JSON selon le schéma déclaré pour ce topic.
    - Conserve le timestamp d'ingestion Kafka (utile pour le monitoring).
    - Aplati les champs de l'événement au niveau racine du DataFrame.

    Lève une KeyError si le topic n'a pas de schéma déclaré.
    """
    if topic not in SCHEMA_BY_TOPIC:
        raise KeyError(f"Aucun schéma déclaré pour le topic '{topic}'")

    schema = SCHEMA_BY_TOPIC[topic]

    return (
        raw_df
        # La clé Kafka (bytes) -> string : notre clé de partition métier.
        .withColumn("kafka_key", F.col("key").cast("string"))
        # Le timestamp d'arrivée dans Kafka (pour observabilité).
        .withColumnRenamed("timestamp", "kafka_timestamp")
        # La valeur (bytes) -> string JSON, puis parsée selon le schéma.
        .withColumn("json_str", F.col("value").cast("string"))
        .withColumn("event", F.from_json(F.col("json_str"), schema))
        # On remonte les champs de l'événement au niveau racine.
        .select(
            "kafka_key",
            "kafka_timestamp",
            "event.*",
        )
    )