"""
Tests unitaires des métriques de déplacement (add_movement_metrics).
Utilise la fixture 'spark' de conftest.py.

Lancement : pytest tests/streaming/test_metrics.py -v
"""
from datetime import datetime

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from streaming.jobs.metrics import add_movement_metrics


_GPS_METRICS_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
])


def _make_df(spark, rows):
    return spark.createDataFrame(rows, _GPS_METRICS_SCHEMA)


def test_premiere_position_metriques_null(spark):
    """La première position d'un véhicule a des métriques nulles (pas de précédent)."""
    rows = [
        ("veh-1", 45.5, -73.6, datetime(2026, 8, 1, 12, 0, 0)),
    ]
    df = _make_df(spark, rows)
    result = add_movement_metrics(df).collect()[0]

    assert result["distance_km"] is None
    assert result["computed_speed_kmh"] is None


def test_deplacement_calcule_distance(spark):
    """Un véhicule qui bouge a une distance > 0 sur la 2e position."""
    rows = [
        ("veh-1", 45.5000, -73.6000, datetime(2026, 8, 1, 12, 0, 0)),
        ("veh-1", 45.5010, -73.5990, datetime(2026, 8, 1, 12, 0, 2)),
    ]
    df = _make_df(spark, rows)
    rows_out = add_movement_metrics(df).orderBy("event_time").collect()

    # 2e ligne : distance calculée non nulle et positive.
    assert rows_out[1]["distance_km"] is not None
    assert rows_out[1]["distance_km"] > 0
    assert rows_out[1]["elapsed_seconds"] == 2


def test_immobile_detecte(spark):
    """Un véhicule qui ne bouge pas est marqué immobile."""
    rows = [
        ("veh-1", 45.5, -73.6, datetime(2026, 8, 1, 12, 0, 0)),
        ("veh-1", 45.5, -73.6, datetime(2026, 8, 1, 12, 0, 2)),  # même position
    ]
    df = _make_df(spark, rows)
    rows_out = add_movement_metrics(df).orderBy("event_time").collect()

    assert rows_out[1]["distance_km"] == 0.0
    assert rows_out[1]["is_immobile"] is True


def test_partition_par_vehicule(spark):
    """Les métriques sont calculées par véhicule (pas de mélange entre véhicules)."""
    rows = [
        ("veh-1", 45.5, -73.6, datetime(2026, 8, 1, 12, 0, 0)),
        ("veh-2", 45.9, -74.0, datetime(2026, 8, 1, 12, 0, 1)),  # autre véhicule
        ("veh-1", 45.501, -73.599, datetime(2026, 8, 1, 12, 0, 2)),
    ]
    df = _make_df(spark, rows)
    result = add_movement_metrics(df).collect()

    # veh-2 n'a qu'une position : ses métriques doivent être nulles
    # (il ne doit PAS être comparé à veh-1).
    veh2 = [r for r in result if r["vehicle_id"] == "veh-2"][0]
    assert veh2["distance_km"] is None