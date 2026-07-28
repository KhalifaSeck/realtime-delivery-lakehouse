#!/usr/bin/env bash
# ============================================================
# Création des topics Kafka du projet.
# Idempotent : si un topic existe déjà, on le signale sans échouer
# (--if-not-exists). À relancer sans risque.
#
# Usage : ./scripts/create_topics.sh
# Prérequis : le conteneur delivery-kafka doit tourner (docker compose up -d).
# ============================================================

set -euo pipefail

CONTAINER="delivery-kafka"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"
BOOTSTRAP="localhost:9092"

# Nombre de partitions par topic.
# 3 partitions : permet le parallélisme côté Spark tout en gardant
# l'ordre par clé (vehicle_id / package_id / driver_id) au sein d'une partition.
PARTITIONS=3
REPLICATION=1   # mono-nœud en local ; passera à 3 sur AKS (Brique 9)

# Liste des topics à créer.
TOPICS=(
  "gps_positions"
  "delivery_events"
  "orders"
  "driver_events"
)

echo "Création des topics sur ${BOOTSTRAP} (partitions=${PARTITIONS}, réplication=${REPLICATION})..."

for topic in "${TOPICS[@]}"; do
  docker exec "${CONTAINER}" "${KAFKA_BIN}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
  echo "  ✓ ${topic}"
done

echo ""
echo "Topics existants :"
docker exec "${CONTAINER}" "${KAFKA_BIN}" \
  --bootstrap-server "${BOOTSTRAP}" \
  --list