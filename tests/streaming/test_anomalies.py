"""
Tests unitaires de la détection d'anomalies (add_anomaly_flags).
Utilise la fixture 'spark' de conftest.py.

Lancement : pytest tests/streaming/test_anomalies.py -v
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)

from streaming.jobs.anomalies import add_anomaly_flags


# Colonnes attendues en entrée (sortie de add_movement_metrics).
_ANOMALY_INPUT_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("computed_speed_kmh", DoubleType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("is_immobile", BooleanType(), True),
])


def _make_df(spark, rows):
    return spark.createDataFrame(rows, _ANOMALY_INPUT_SCHEMA)


def _anomalies_for(spark, row):
    """Retourne la liste anomaly_types pour une seule ligne."""
    df = _make_df(spark, [row])
    result = add_anomaly_flags(df).collect()[0]
    return result["anomaly_types"], result["is_anomaly"]


def test_normal_aucune_anomalie(spark):
    """Véhicule normal : aucune anomalie."""
    types, is_anomaly = _anomalies_for(spark, ("veh-1", 30.0, 32.0, 0.02, False))
    assert types == []
    assert is_anomaly is False


def test_speed_too_high(spark):
    """Vitesse calculée aberrante -> SPEED_TOO_HIGH."""
    types, is_anomaly = _anomalies_for(spark, ("veh-1", 30.0, 200.0, 0.11, False))
    assert "SPEED_TOO_HIGH" in types
    assert is_anomaly is True


def test_speed_mismatch(spark):
    """Gros écart déclaré/calculé -> SPEED_MISMATCH."""
    types, _ = _anomalies_for(spark, ("veh-1", 30.0, 90.0, 0.05, False))
    assert "SPEED_MISMATCH" in types


def test_immobile(spark):
    """Véhicule immobile -> IMMOBILE."""
    types, _ = _anomalies_for(spark, ("veh-1", 0.0, 0.0, 0.0, True))
    assert "IMMOBILE" in types


def test_gps_jump(spark):
    """Distance irréaliste -> GPS_JUMP."""
    types, _ = _anomalies_for(spark, ("veh-1", 30.0, 40.0, 3.5, False))
    assert "GPS_JUMP" in types


def test_anomalies_multiples(spark):
    """Une ligne peut cumuler plusieurs anomalies."""
    # Vitesse très haute ET écart énorme : SPEED_TOO_HIGH + SPEED_MISMATCH.
    types, is_anomaly = _anomalies_for(spark, ("veh-1", 30.0, 200.0, 0.11, False))
    assert "SPEED_TOO_HIGH" in types
    assert "SPEED_MISMATCH" in types
    assert len(types) >= 2
    assert is_anomaly is True