"""
Validation des événements lus depuis Kafka.

Sépare les événements en deux flux :
  - les événements VALIDES (clés obligatoires présentes, JSON bien parsé),
  - les événements INVALIDES ("dead-letter"), à des fins d'inspection.

Un message peut être invalide pour deux raisons :
  1. Le JSON n'a pas pu être parsé (from_json a renvoyé null partout) :
     message corrompu ou ne respectant pas le schéma.
  2. Une clé métier obligatoire est absente (ex. vehicle_id manquant) :
     l'événement est inexploitable en aval.

On ne jette jamais silencieusement : les invalides partent dans un flux
séparé qu'on pourra logguer ou stocker (dead-letter queue).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Clés métier obligatoires par topic : sans elles, l'événement est inexploitable.
_REQUIRED_KEYS = {
    "gps_positions": ["vehicle_id", "driver_id"],
    "delivery_events": ["package_id", "order_id", "vehicle_id"],
    "orders": ["order_id", "package_id"],
    "driver_events": ["driver_id", "vehicle_id"],
}


def _validity_condition(topic: str):
    """
    Construit la condition booléenne Spark caractérisant un événement valide
    pour un topic : toutes ses clés obligatoires sont non nulles.
    """
    required = _REQUIRED_KEYS.get(topic, [])
    if not required:
        # Aucun requis déclaré : on considère tout comme valide.
        return F.lit(True)

    condition = F.col(required[0]).isNotNull()
    for key in required[1:]:
        condition = condition & F.col(key).isNotNull()
    return condition


def split_valid_invalid(parsed_df: DataFrame, topic: str) -> tuple[DataFrame, DataFrame]:
    """
    Sépare un DataFrame d'événements parsés en (valides, invalides).

    - valides   : toutes les clés obligatoires du topic sont présentes.
    - invalides : au moins une clé obligatoire manque (JSON mal parsé
                  ou champ absent). Enrichis d'une colonne 'reject_reason'
                  et 'reject_topic' pour l'inspection.

    Retourne un tuple (valid_df, invalid_df).
    """
    condition = _validity_condition(topic)

    valid_df = parsed_df.filter(condition)

    invalid_df = (
        parsed_df
        .filter(~condition)
        .withColumn("reject_reason", F.lit("missing_required_key"))
        .withColumn("reject_topic", F.lit(topic))
    )

    return valid_df, invalid_df


def count_validity(parsed_df: DataFrame, topic: str) -> DataFrame:
    """
    Utilitaire d'observabilité (batch/debug uniquement) :
    compte valides vs invalides. À NE PAS utiliser sur un flux streaming
    (count() est une action ; réservé au debug sur données statiques).
    """
    condition = _validity_condition(topic)
    return (
        parsed_df
        .withColumn("is_valid", condition)
        .groupBy("is_valid")
        .count()
    )