"""
Tests unitaires du nettoyage streaming (cleaning).
Utilise la fixture 'spark' de conftest.py.

Lancement : pytest tests/streaming/test_cleaning.py -v
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

from streaming.jobs.cleaning import (
    add_event_time,
    normalize_status,
    filter_valid_coordinates,
    clean_events,
)


# Schéma delivery explicite (statut + coordonnées, pour tester tous les cas).
_DELIVERY_TEST_SCHEMA = StructType([
    StructField("package_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


def _make_delivery_df(spark, rows):
    return spark.createDataFrame(rows, _DELIVERY_TEST_SCHEMA)


def test_add_event_time_cree_timestamp(spark):
    """add_event_time convertit la chaîne ISO en colonne timestamp non nulle."""
    rows = [("pkg-1", "veh-1", "delivered", 45.5, -73.6, "2026-08-01T21:44:00+00:00")]
    df = _make_delivery_df(spark, rows)

    result = add_event_time(df)

    assert "event_time" in result.columns
    row = result.collect()[0]
    assert row["event_time"] is not None


def test_add_event_time_timestamp_invalide_donne_null(spark):
    """Un timestamp non parsable produit event_time = null (pas d'exception)."""
    rows = [("pkg-1", "veh-1", "delivered", 45.5, -73.6, "pas-une-date")]
    df = _make_delivery_df(spark, rows)

    result = add_event_time(df)

    row = result.collect()[0]
    assert row["event_time"] is None


def test_normalize_status_minuscule_trim(spark):
    """normalize_status met en minuscules et retire les espaces."""
    rows = [("pkg-1", "veh-1", "  PICKED_UP  ", 45.5, -73.6, "2026-08-01T21:44:00+00:00")]
    df = _make_delivery_df(spark, rows)

    result = normalize_status(df)

    row = result.collect()[0]
    assert row["status"] == "picked_up"


def test_filter_coordonnees_aberrantes(spark):
    """Une coordonnée hors bornes (Paris) est filtrée."""
    rows = [
        ("pkg-1", "veh-1", "delivered", 45.5, -73.6, "2026-08-01T21:44:00+00:00"),  # Montréal, gardé
        ("pkg-2", "veh-2", "delivered", 48.9,  2.35, "2026-08-01T21:44:02+00:00"),  # Paris, filtré
    ]
    df = _make_delivery_df(spark, rows)

    result = filter_valid_coordinates(df)

    assert result.count() == 1
    assert result.collect()[0]["package_id"] == "pkg-1"


def test_filter_coordonnees_null_conservees(spark):
    """Les lignes sans coordonnées (null) sont conservées."""
    rows = [
        ("pkg-1", "veh-1", "created", None, None, "2026-08-01T21:44:00+00:00"),
    ]
    df = _make_delivery_df(spark, rows)

    result = filter_valid_coordinates(df)

    assert result.count() == 1


def test_clean_events_pipeline_complet(spark):
    """Le pipeline complet : event_time ajouté, statut normalisé, Paris filtré."""
    rows = [
        ("pkg-1", "veh-1", "PICKED_UP ", 45.5, -73.6, "2026-08-01T21:44:00+00:00"),  # gardé, normalisé
        ("pkg-2", "veh-2", "delivered",  48.9,  2.35, "2026-08-01T21:44:02+00:00"),  # Paris, filtré
        ("pkg-3", "veh-3", "created",    None,  None,  "2026-08-01T21:44:04+00:00"),  # null, gardé
    ]
    df = _make_delivery_df(spark, rows)

    result = clean_events(df)

    # 2 lignes restantes (Paris filtré).
    assert result.count() == 2
    # event_time présent.
    assert "event_time" in result.columns
    # Statut normalisé sur pkg-1.
    pkg1 = result.filter(result.package_id == "pkg-1").collect()[0]
    assert pkg1["status"] == "picked_up"