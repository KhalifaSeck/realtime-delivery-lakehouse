# Realtime Delivery Lakehouse

Plateforme de suivi de livraisons en temps réel, de bout en bout : simulation d'événements, ingestion streaming, serving temps réel et analytique historique.

## Architecture

~~~
Terraform ──► Infrastructure Azure (AKS, ADLS Gen2, réseau, secrets) + Snowflake

Simulateur Python ──► Kafka (4 topics) ──► PySpark Structured Streaming
                                                  │
                              ┌───────────────────┴───────────────────┐
                              ▼                                       ▼
                           Redis                              ADLS Gen2 (Parquet)
                    (état courant véhicule)              (historique partitionné)
                              │                                       │
                              ▼                                       ▼
                      API FastAPI temps réel                     Snowflake
                    (position / statut colis)                        │
                                                                     ▼
                                                              dbt (staging → marts)
                                                                     │
                                                                     ▼
                                                                 Power BI
~~~

## Stack technique

| Couche | Technologie |
|---|---|
| Infrastructure as Code | Terraform (Azure, Snowflake) |
| Génération d'événements | Simulateur Python (chauffeurs, GPS, colis, commandes) |
| Messaging | Apache Kafka (KRaft en local, Strimzi sur AKS) |
| Traitement streaming | PySpark Structured Streaming |
| État temps réel | Redis |
| Data Lake | Azure Data Lake Storage Gen2 (Parquet partitionné) |
| Entrepôt | Snowflake |
| Transformation | dbt (tests de qualité inclus) |
| API | FastAPI |
| Visualisation | Power BI |
| Orchestration conteneurs | Docker Compose (dev), Kubernetes/AKS (prod) |

## Topics Kafka

| Topic | Clé de partition | Contenu |
|---|---|---|
| `gps_positions` | `vehicle_id` | Positions GPS des véhicules (ordre garanti par véhicule) |
| `delivery_events` | `package_id` | Événements de livraison (pickup, transit, delivered) |
| `orders` | `package_id` | Création et mise à jour des commandes |
| `driver_events` | `driver_id` | Connexions, pauses, fins de quart |

## Démarrage rapide (dev local)

~~~bash
# 1. Configuration
cp .env.example .env

# 2. Démarrer l'infrastructure locale (Kafka, Redis)
docker compose up -d

# 3. Créer les topics
./scripts/create_topics.sh

# 4. Lancer le simulateur
python -m simulator.main

# 5. Lancer le job streaming (autre terminal)
python -m streaming.main

# 6. Lancer l'API (autre terminal)
uvicorn api.main:app --reload
~~~

## Structure du projet

~~~
infra/          Terraform (modules aks, adls, snowflake, network)
simulator/      Simulateur Python d'événements de livraison
streaming/      Jobs PySpark Structured Streaming (validation, enrichissement, métriques)
api/            API FastAPI de suivi temps réel (lecture Redis)
snowflake/      DDL et ingestion (stage externe ADLS, COPY INTO)
dbt/            Modèles staging → marts, tests de qualité
powerbi/        Rapport Power BI et documentation des mesures DAX
k8s/            Manifests Kubernetes pour le déploiement AKS
scripts/        Scripts utilitaires
tests/          Tests unitaires (simulateur, streaming, API)
~~~

## Plan de construction

- [x] **Brique 0** — Structure du repo, configuration de base
- [x] **Brique 1** — Simulateur Python + producteurs Kafka
- [ ] **Brique 2** — Spark Structured Streaming : validation et nettoyage
- [ ] **Brique 3** — Enrichissement (jointures), métriques, anomalies
- [ ] **Brique 4** — Sinks Redis et ADLS Gen2
- [ ] **Brique 5** — API FastAPI temps réel
- [ ] **Brique 6** — Snowflake : stage externe et ingestion
- [ ] **Brique 7** — dbt : modèles staging et marts, tests
- [ ] **Brique 8** — Power BI
- [ ] **Brique 9** — Terraform + migration AKS
