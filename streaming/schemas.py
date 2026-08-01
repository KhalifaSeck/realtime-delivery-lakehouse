"""
Schémas explicites des événements lus depuis Kafka.

Chaque schéma correspond EXACTEMENT aux champs produits par le simulateur
(cf. simulator/models.py). Spark utilise ces schémas pour parser le JSON
des messages Kafka de façon fiable, sans inférence coûteuse ni ambiguë.

Rappel des topics et de leurs clés de partition :
    gps_positions   (clé vehicle_id)  -> GPS_SCHEMA
    delivery_events (clé package_id)  -> DELIVERY_SCHEMA
    orders          (clé package_id)  -> ORDER_SCHEMA
    driver_events   (clé driver_id)   -> DRIVER_SCHEMA
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# ------------------------------------------------------------
# GPS : position instantanée d'un véhicule (topic gps_positions)
# Correspond à GpsEvent dans simulator/models.py
# ------------------------------------------------------------
GPS_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("driver_id", StringType(), nullable=False),
    StructField("lat", DoubleType(), nullable=False),
    StructField("lon", DoubleType(), nullable=False),
    StructField("speed_kmh", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
])


# ------------------------------------------------------------
# Livraison : changement de statut d'un colis (topic delivery_events)
# Correspond à DeliveryEvent dans simulator/models.py
# ------------------------------------------------------------
DELIVERY_SCHEMA = StructType([
    StructField("package_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lon", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
])


# ------------------------------------------------------------
# Commande : création/màj d'une commande (topic orders)
# Correspond à OrderEvent dans simulator/models.py
# ------------------------------------------------------------
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("package_id", StringType(), nullable=False),
    StructField("dest_lat", DoubleType(), nullable=True),
    StructField("dest_lon", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
])


# ------------------------------------------------------------
# Chauffeur : changement de statut (topic driver_events)
# Correspond à DriverEvent dans simulator/models.py
# ------------------------------------------------------------
DRIVER_SCHEMA = StructType([
    StructField("driver_id", StringType(), nullable=False),
    StructField("vehicle_id", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lon", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
])


# Table de correspondance topic -> schéma, pratique pour le dispatch.
SCHEMA_BY_TOPIC = {
    "gps_positions": GPS_SCHEMA,
    "delivery_events": DELIVERY_SCHEMA,
    "orders": ORDER_SCHEMA,
    "driver_events": DRIVER_SCHEMA,
}