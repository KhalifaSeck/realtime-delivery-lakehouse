"""
Tests unitaires de la validation streaming (split_valid_invalid).
Utilise la fixture 'spark' de conftest.py.

Lancement : pytest tests/streaming/test_validation.py -v
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

from streaming.jobs.validation import split_valid_invalid


# Schéma explicite : évite l'échec d'inférence quand une colonne est
# entièrement nulle (createDataFrame ne peut pas deviner le type sinon).
_GPS_TEST_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# Schéma explicite pour les événements 'orders'.
_ORDER_TEST_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("package_id", StringType(), True),
    StructField("dest_lat", DoubleType(), True),
    StructField("dest_lon", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def _make_gps_df(spark, rows):
    """Fabrique un DataFrame GPS avec schéma explicite (pas d'inférence)."""
    return spark.createDataFrame(rows, _GPS_TEST_SCHEMA)


def _make_order_df(spark, rows):
    """Fabrique un DataFrame orders avec schéma explicite."""
    return spark.createDataFrame(rows, _ORDER_TEST_SCHEMA)


def test_tous_valides(spark):
    """Tous les événements ont leurs clés : aucun invalide."""
    rows = [
        ("veh-1", "drv-1", 45.5, -73.6, 30.0, "2026-08-01T21:44:00+00:00"),
        ("veh-2", "drv-2", 45.6, -73.7, 25.0, "2026-08-01T21:44:02+00:00"),
    ]
    df = _make_gps_df(spark, rows)
    valid, invalid = split_valid_invalid(df, "gps_positions")

    assert valid.count() == 2
    assert invalid.count() == 0


def test_vehicle_id_manquant_invalide(spark):
    """Un événement sans vehicle_id part dans les invalides."""
    rows = [
        ("veh-1", "drv-1", 45.5, -73.6, 30.0, "2026-08-01T21:44:00+00:00"),
        (None,    "drv-2", 45.6, -73.7, 25.0, "2026-08-01T21:44:02+00:00"),
    ]
    df = _make_gps_df(spark, rows)
    valid, invalid = split_valid_invalid(df, "gps_positions")

    assert valid.count() == 1
    assert invalid.count() == 1


def test_driver_id_manquant_invalide(spark):
    """Un événement sans driver_id part dans les invalides (GPS exige les deux)."""
    rows = [
        ("veh-1", None, 45.5, -73.6, 30.0, "2026-08-01T21:44:00+00:00"),
    ]
    df = _make_gps_df(spark, rows)
    valid, invalid = split_valid_invalid(df, "gps_positions")

    assert valid.count() == 0
    assert invalid.count() == 1


def test_invalides_ont_reject_reason(spark):
    """Les invalides sont annotés reject_reason et reject_topic."""
    rows = [
        (None, "drv-1", 45.5, -73.6, 30.0, "2026-08-01T21:44:00+00:00"),
    ]
    df = _make_gps_df(spark, rows)
    _valid, invalid = split_valid_invalid(df, "gps_positions")

    row = invalid.collect()[0]
    assert row["reject_reason"] == "missing_required_key"
    assert row["reject_topic"] == "gps_positions"


def test_orders_cles_specifiques(spark):
    """Les orders exigent order_id ET package_id."""
    rows = [
        ("ord-1", "pkg-1", 45.5, -73.6, "created", "2026-08-01T21:44:00+00:00"),  # valide
        ("ord-2", None,    45.6, -73.7, "created", "2026-08-01T21:44:02+00:00"),  # invalide
    ]
    df = _make_order_df(spark, rows)
    valid, invalid = split_valid_invalid(df, "orders")

    assert valid.count() == 1
    assert invalid.count() == 1