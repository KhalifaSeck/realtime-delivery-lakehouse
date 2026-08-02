"""
Construction générique du pipeline amont d'un topic.

Factorise la chaîne commune à tous les topics :
    lecture Kafka -> parsing -> validation -> nettoyage

Retourne le flux valide (nettoyé) et le flux invalide (dead-letter),
que main.py dirige ensuite vers les bons sinks.
"""
from pyspark.sql import SparkSession, DataFrame

from streaming.jobs.read_kafka import read_topic_raw, parse_events
from streaming.jobs.validation import split_valid_invalid
from streaming.jobs.cleaning import clean_events


def build_topic_pipeline(spark: SparkSession, topic: str) -> tuple[DataFrame, DataFrame]:
    """
    Construit le pipeline amont d'un topic (sans démarrer de requête).

    Retourne (valid_clean, invalid) :
      - valid_clean : événements valides, nettoyés (event_time, etc.) ;
      - invalid     : événements rejetés (avec reject_reason, reject_topic).

    Le nettoyage n'est appliqué qu'aux valides ; les invalides partent
    tels quels vers la DLQ (on veut les inspecter dans leur état brut).
    """
    raw = read_topic_raw(spark, topic)
    parsed = parse_events(raw, topic)
    valid, invalid = split_valid_invalid(parsed, topic)
    valid_clean = clean_events(valid)
    return valid_clean, invalid