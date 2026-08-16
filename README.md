# 🚚 Real-Time Delivery Lakehouse

![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-3.3.4-E25A1C?logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-3.7.0-231F20?logo=apachekafka&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-844FBA?logo=terraform&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-1.32-326CE5?logo=kubernetes&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?logo=microsoftazure&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-11.1-F46800?logo=grafana&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-2.53-E6522C?logo=prometheus&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?logo=powerbi&logoColor=black)
![Airflow](https://img.shields.io/badge/Airflow-Astronomer-017CEE?logo=apacheairflow&logoColor=white)
![License](https://img.shields.io/badge/License-Educational-green)

A **production-grade, end-to-end data engineering platform** that simulates a real-time delivery fleet, streams events through Kafka, processes them with Spark, stores data in Azure ADLS Gen2, transforms it with dbt in Snowflake, and visualizes KPIs in Power BI — fully orchestrated on Kubernetes.

> Built as a portfolio project demonstrating mastery of the modern data stack, from event simulation to executive dashboards, with zero manual intervention.

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  KUBERNETES (namespace: delivery)                                    │
│                                                                      │
│  ┌───────────┐   ┌───────┐   ┌───────────────────┐                  │
│  │ Simulator │──▶│ Kafka │──▶│ Spark Streaming   │                  │
│  │ (15 drv)  │   │ KRaft │   │  (8 streaming     │                  │
│  └───────────┘   └───────┘   │   queries)        │                  │
│                               │                   │                  │
│                               │  ┌──▶ Redis       │                  │
│                               │  │   (live state) │                  │
│                               │  │                │                  │
│                               │  ├──▶ Local Lake  │                  │
│                               │  │   (debug)      │                  │
│                               │  │                │                  │
│                               │  └──▶ ADLS Gen2 ☁ │                  │
│                               └───────────────────┘                  │
│                                                                      │
│  ┌───────────┐   ┌────────────┐   ┌─────────┐                      │
│  │ FastAPI   │──▶│ Prometheus │──▶│ Grafana │                       │
│  │ /metrics  │   │            │   │         │                       │
│  └───────────┘   └────────────┘   └─────────┘                      │
│                                                                      │
│  ┌─────────────────────────────────────────────┐                    │
│  │  CronJob (every 5 min)                       │                    │
│  │  ┌──────────┐  ┌─────────┐  ┌────────────┐  │                    │
│  │  │COPY INTO │─▶│dbt run  │─▶│dbt test    │  │                    │
│  │  │Snowflake │  │staging  │  │staging     │  │                    │
│  │  └──────────┘  │+ marts  │  │+ marts     │  │                    │
│  │                └─────────┘  └────────────┘  │                    │
│  └─────────────────────────────────────────────┘                    │
└──────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  CLOUD                                                               │
│                                                                      │
│  Azure ADLS Gen2          Snowflake                    Power BI      │
│  ┌──────────────┐    ┌─────────────────┐          ┌──────────────┐  │
│  │ raw-events/  │───▶│ RAW (4 tables)  │          │ Dashboard    │  │
│  │  gps/        │    │ STAGING (4 views)│─────────▶│ KPIs         │  │
│  │  delivery/   │    │ MARTS (5 tables) │          │ Trends       │  │
│  │  order/      │    └─────────────────┘          │ Fleet        │  │
│  │  driver/     │                                  └──────────────┘  │
│  └──────────────┘                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

### Real-Time Streaming Pipeline
- **15 simulated drivers** generating GPS pings, delivery events, orders, and status changes
- **Apache Kafka** (KRaft mode, no Zookeeper) as the event backbone with 4 topics
- **Spark Structured Streaming** with 8 concurrent queries processing events in real time
- **Dual-write pattern**: local Parquet (debug) + Azure ADLS Gen2 (cloud) simultaneously

### Cloud-Native Data Warehouse
- **Azure ADLS Gen2** as the cloud data lake (Terraform-provisioned)
- **Snowflake** as the analytical warehouse with external stage pointing to ADLS
- **COPY INTO** with `MATCH_BY_COLUMN_NAME` and `PATTERN` filtering for robust ingestion
- **Two SAS tokens** (read for Snowflake, write for Python) following least-privilege principle

### dbt Transformations (Medallion Architecture)
- **STAGING layer** (Silver): 4 views with type casting, deduplication, validation
- **MARTS layer** (Gold): 3 dimensions + 2 fact tables for Power BI consumption
- **ASOF JOIN** to reconstruct the package-to-driver relationship (temporal join)
- **41 automated tests** (not_null, unique, accepted_values)
- **Custom `generate_schema_name` macro** to avoid dbt's default schema prefixing

### Full Observability Stack
- **FastAPI** exposing Prometheus metrics (data quality score, fleet status, event counts)
- **Prometheus** scraping metrics every 10 seconds
- **Grafana** with a 5-panel dashboard (quality gauge, fleet distribution, speed trends)
- **Great Expectations** integration with TTL caching (300s) for data quality validation

### Kubernetes Orchestration
- **7 Deployments**: Simulator, Kafka, Spark, Redis, API, Prometheus, Grafana
- **1 CronJob**: batch pipeline (COPY INTO + dbt) running every 5 minutes
- **ConfigMaps and Secrets** for centralized, secure configuration
- **Resource limits** on every pod for predictable scheduling

### Infrastructure as Code
- **Terraform** provisioning Azure (Resource Group, Storage Account, ADLS Gen2 container) and Snowflake (Database, Schemas, Warehouse, Role, Tables, Stage, File Format, Grants)
- **Idempotent** — `terraform apply` is safe to run multiple times

### Power BI Analytics
- Connected to Snowflake MARTS via DirectImport
- KPI cards, trend lines, fleet distribution charts, delivery detail tables
- Dimensional model with `dim_date` for calendar-based filtering

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Simulation** | Python 3.10 | 15 concurrent drivers generating 4 event types |
| **Messaging** | Apache Kafka 3.7.0 (KRaft) | Event streaming with 4 topics |
| **Processing** | Spark Structured Streaming 3.3.4 | Real-time ETL with 8 queries |
| **Cache** | Redis 7.2 | Live vehicle state for dashboards |
| **Cloud Lake** | Azure ADLS Gen2 | Parquet storage (Terraform-provisioned) |
| **Warehouse** | Snowflake | Analytical queries, staging, marts |
| **Transformation** | dbt Core 1.12 | Medallion architecture (STAGING → MARTS) |
| **Orchestration** | Kubernetes CronJob | Batch pipeline every 5 min |
| **Monitoring** | Prometheus + Grafana | Metrics collection and visualization |
| **API** | FastAPI | Metrics endpoint for Prometheus |
| **Visualization** | Power BI Desktop | Executive dashboards |
| **IaC** | Terraform | Azure + Snowflake provisioning |
| **Containers** | Docker + Kubernetes | Local K8s via Docker Desktop |
| **Orchestrator** | Airflow (Astronomer) | Alternative DAG-based orchestration |

---

## 📁 Project Structure

```
realtime-delivery-lakehouse/
│
├── airflow/                          # Airflow orchestration (Astronomer)
│   ├── Dockerfile                    #   Astro Runtime base image
│   ├── requirements.txt             #   Python deps (dbt, snowflake, cosmos)
│   ├── packages.txt                 #   System deps (git)
│   ├── dags/
│   │   └── delivery_pipeline_realtime.py   # DAG: COPY INTO + dbt (*/5 min)
│   └── include/
│       ├── ingestion/               #   Copy of ingestion scripts
│       └── dbt/                     #   Copy of dbt project + profiles
│
├── api/                              # FastAPI metrics server
│   ├── Dockerfile                    #   Python 3.10-slim + gcc
│   ├── metrics.py                   #   Prometheus metrics + GX cache
│   └── requirements.txt
│
├── delivery_dbt/                     # dbt project (Snowflake)
│   ├── dbt_project.yml              #   2-layer config (staging + marts)
│   ├── macros/
│   │   └── generate_schema_name.sql #   Override schema prefixing
│   └── models/
│       ├── staging/                  #   Silver layer (4 views)
│       │   ├── _sources.yml         #     RAW table declarations + tests
│       │   ├── _models.yml          #     Model docs + tests
│       │   ├── stg_gps_events.sql
│       │   ├── stg_delivery_events.sql
│       │   ├── stg_orders.sql
│       │   └── stg_driver_events.sql
│       └── marts/                    #   Gold layer (5 tables)
│           ├── _models.yml
│           ├── dim_drivers.sql       #     Driver dimension + success rates
│           ├── dim_vehicles.sql      #     Vehicle dimension + GPS aggregates
│           ├── dim_date.sql          #     Calendar dimension (GENERATOR)
│           ├── fct_deliveries.sql    #     Delivery facts + ASOF JOIN
│           └── fct_daily_activity.sql #    Fleet daily KPIs
│
├── ingestion/                        # Batch ingestion scripts
│   ├── __init__.py
│   ├── upload_to_adls.py            #   Lake local → ADLS Gen2 (legacy)
│   └── copy_to_snowflake.py         #   COPY INTO: ADLS stage → Snowflake RAW
│
├── k8s/                              # Kubernetes manifests
│   ├── namespace.yaml
│   ├── configmap.yaml               #   Non-sensitive shared config
│   ├── secrets.yaml                 #   Base64 credentials (gitignored)
│   ├── deploy.ps1                   #   One-command deployment
│   ├── teardown.ps1                 #   Cleanup script
│   ├── kafka/
│   │   ├── deployment.yaml          #     KRaft mode, tcpSocket probe
│   │   └── service.yaml             #     ClusterIP (9092 + 29092)
│   ├── redis/
│   │   ├── deployment.yaml          #     Alpine, appendonly
│   │   └── service.yaml             #     ClusterIP (6379)
│   ├── spark/
│   │   └── deployment.yaml          #     Multi-stage image, SharedKey auth
│   ├── simulator/
│   │   └── deployment.yaml          #     15 drivers, kafka-service:29092
│   ├── api/
│   │   ├── deployment.yaml          #     delivery-api:latest, NodePort
│   │   └── service.yaml             #     NodePort 30080
│   ├── prometheus/
│   │   ├── configmap.yaml           #     Scrape config for api-service
│   │   ├── deployment.yaml
│   │   └── service.yaml             #     NodePort 30090
│   ├── grafana/
│   │   ├── deployment.yaml          #     Grafana 11.1, admin/admin
│   │   └── service.yaml             #     NodePort 30030
│   └── pipeline/
│       └── cronjob.yaml             #     Batch pipeline */5 * * * *
│
├── logs/                             # Application logs (gitignored)
│
├── observability/                    # Monitoring configuration
│   ├── prometheus.yml               #   Scrape targets + intervals
│   └── grafana/
│       └── provisioning/            #   Auto-provisioned datasources
│
├── pipeline/                         # Batch pipeline container (K8s CronJob)
│   ├── Dockerfile                   #   Python 3.10 + dbt + snowflake
│   ├── run_pipeline.sh              #   5-step orchestration script
│   └── dbt_profiles/
│       └── profiles.yml             #   env_var() based credentials
│
├── quality/                          # Data quality (Great Expectations)
│   └── validate_lake.py             #   Lake validation + metrics
│
├── scripts/                          # Utility scripts
│
├── simulator/                        # Event generator
│   ├── Dockerfile                   #   Python 3.10-slim
│   ├── main.py                      #   15 concurrent drivers
│   └── requirements.txt             #   kafka-python, python-dotenv
│
├── snowflake/                        # Snowflake SQL worksheets
│   └── worksheet_delivery.sql       #   Manual queries + validation
│
├── streaming/                        # Spark Structured Streaming
│   ├── config.py                    #   SparkSession + ADLS SharedKey
│   ├── main.py                      #   Orchestrates 8 streaming queries
│   ├── schemas.py                   #   Kafka message schemas
│   ├── Dockerfile                   #   Multi-stage: Java 11 + Python 3.10
│   ├── requirements.txt             #   pyspark, redis, python-dotenv
│   ├── __init__.py
│   ├── jobs/                        #   Processing pipeline
│   │   ├── pipeline.py              #     Generic topic pipeline builder
│   │   ├── read_kafka.py            #     Kafka source reader
│   │   ├── validation.py            #     Schema validation + DLQ split
│   │   ├── cleaning.py              #     Data cleaning
│   │   ├── enrichment.py            #     GPS enrichment (speed, anomalies)
│   │   ├── anomalies.py             #     Anomaly detection rules
│   │   └── metrics.py               #     Streaming metrics
│   └── sinks/                       #   Output destinations
│       ├── adls_sink.py             #     Dual-write: local + ADLS Gen2
│       ├── redis_sink.py            #     Live vehicle state to Redis
│       └── deadletter_sink.py       #     Invalid events quarantine
│
├── terraform/                        # Infrastructure as Code
│   ├── providers.tf                 #   Azure + Snowflake providers
│   ├── variables.tf                 #   Project variables
│   ├── azure_adls.tf                #   ADLS Gen2 + 2 SAS tokens
│   ├── snowflake_core.tf            #   Database, warehouse, role, grants
│   ├── snowflake_tables.tf          #   4 RAW tables
│   └── snowflake_stage.tf           #   External stage + Parquet format
│
├── tests/                            # Unit and integration tests
│
├── .env                              # Secrets (gitignored)
├── .env.example                      # Template for .env
├── .gitignore                        # Comprehensive ignore rules
├── docker-compose.yml                # Local dev stack (pre-K8s)
├── LICENSE
├── pyproject.toml                    # Python project config
├── requirements.txt                  # Root Python dependencies
└── README.md                         # This file
```

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop with Kubernetes enabled (4+ CPUs, 8+ GB RAM)
- Python 3.10 (conda environment `dbt-env`)
- Terraform CLI
- Azure account (Student credits work)
- Snowflake trial account
- Power BI Desktop (Windows)

### 1. Clone and configure

```bash
git clone https://github.com/KhalifaSeck/realtime-delivery-lakehouse.git
cd realtime-delivery-lakehouse
cp .env.example .env
# Fill in your credentials in .env
```

### 2. Provision cloud infrastructure

```bash
cd terraform
terraform init
terraform apply    # Creates ADLS Gen2 + Snowflake resources
cd ..
```

### 3. Deploy on Kubernetes

```powershell
# Encode secrets in base64 and fill k8s/secrets.yaml
# [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("your_value"))

.\k8s\deploy.ps1
```

Verify all pods are running:

```bash
kubectl get pods -n delivery
```

Expected:

```
NAME                          READY   STATUS    AGE
api-xxxxx                     1/1     Running   2m
grafana-xxxxx                 1/1     Running   2m
kafka-xxxxx                   1/1     Running   2m
prometheus-xxxxx              1/1     Running   2m
redis-xxxxx                   1/1     Running   2m
simulator-xxxxx               1/1     Running   1m
spark-xxxxx                   1/1     Running   1m
```

### 4. Create Kafka topics

```bash
KAFKA_POD=$(kubectl get pod -l app=kafka -n delivery -o jsonpath='{.items[0].metadata.name}')
for topic in gps_positions delivery_events orders driver_events; do
  kubectl exec $KAFKA_POD -n delivery -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic $topic \
    --partitions 1 --replication-factor 1
done
```

### 5. Verify the automated pipeline

```bash
# CronJob runs every 5 minutes automatically
kubectl get cronjob -n delivery

# Watch a manual run
kubectl create job --from=cronjob/pipeline-job pipeline-manual -n delivery
kubectl logs -f job/pipeline-manual -n delivery
```

### 6. Access dashboards

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:30030 | admin / admin |
| Prometheus | http://localhost:30090 | — |
| API Metrics | http://localhost:30080/metrics | — |

### 7. Connect Power BI

1. Open Power BI Desktop
2. Get Data → Snowflake
3. Server: `<your_account>.snowflakecomputing.com`
4. Warehouse: `DELIVERY_WH`
5. Import 5 tables from `DELIVERY_LAKEHOUSE.MARTS`
6. Refresh (Ctrl+R) to see updated data

---

## 🧪 Data Quality & Testing

### dbt Tests — 41 Automated

| Layer | Tests | Examples |
|---|---|---|
| **Sources** (RAW) | 11 | `not_null` on IDs and timestamps |
| **Staging** | 16 | `not_null`, `unique`, `accepted_values` |
| **Marts** | 14 | `not_null`, `unique`, `accepted_values` on dimensions and facts |

```bash
cd delivery_dbt
dbt test --profiles-dir ~/.dbt     # Run all 41 tests
dbt docs generate && dbt docs serve   # Browse documentation + lineage
```

### Prometheus Metrics

| Metric | Description |
|---|---|
| `delivery_data_quality_score` | Overall quality (0–100%) via Great Expectations |
| `delivery_vehicles_tracked` | Number of vehicles currently tracked |
| `delivery_drivers_by_status` | Driver count by status |
| `delivery_vehicles_avg_speed_kmh` | Fleet average speed |
| `delivery_lake_events_total` | Total events by type in the lake |
| `delivery_lake_last_event_age_seconds` | Freshness indicator per event type |
| `delivery_lake_anomalies_total` | Anomaly count by type |

---

## 📊 Data Model (Star Schema)

```
                    ┌─────────────┐
                    │  dim_date   │
                    │─────────────│
                    │ date_day    │
                    │ year        │
                    │ quarter     │
                    │ month       │
                    │ is_weekend  │
                    │ is_business │
                    │   _day      │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────┐    ┌───────────────┐
│ dim_drivers  │    │   fct_      │    │ dim_vehicles  │
│──────────────│    │ deliveries  │    │───────────────│
│ driver_id    │◄───│─────────────│───▶│ vehicle_id    │
│ current_     │    │ package_id  │    │ n_active_days │
│   status     │    │ order_id    │    │ avg_speed_kmh │
│ success_     │    │ vehicle_id  │    │ total_gps_    │
│   rate_pct   │    │ driver_id   │    │   points      │
│ n_deliveries │    │ final_status│    │ max_speed_    │
│  _completed  │    │ delivered_at│    │   ever_kmh    │
│ active_hours │    │ minutes_    │    └───────────────┘
└──────────────┘    │  order_to_  │
                    │  delivery   │
                    │ is_delivered│
                    └─────────────┘

                    ┌─────────────┐
                    │ fct_daily_  │
                    │  activity   │
                    │─────────────│
                    │activity_date│
                    │n_active_    │
                    │  vehicles   │
                    │n_delivered  │
                    │n_failed     │
                    │daily_success│
                    │  _rate_pct  │
                    │fleet_avg_   │
                    │  speed_kmh  │
                    └─────────────┘
```

### Key Metrics

| Metric | Value | Source |
|---|---|---|
| Total deliveries | 699+ | fct_deliveries |
| Success rate | ~93% | fct_deliveries |
| Active vehicles | 28 | dim_vehicles |
| Active drivers | 32 | dim_drivers |
| Daily activity days | 4+ | fct_daily_activity |
| dbt tests passing | 41/41 | dbt test |

---

## 🏗️ Technical Decisions & Trade-offs

### Why SharedKey instead of SAS for Spark → ADLS?

PySpark 3.3.4 ships with Hadoop 3.3.2, which does not include `FixedSASTokenProvider` (added in Hadoop 3.4.1). Passing a SAS token via Spark config also fails on Windows because `&` characters in the token are interpreted as command separators by `cmd.exe`. SharedKey authentication is compatible with Hadoop 3.3.2 and avoids the shell escaping issue.

> **Production path**: Azure AD Service Principal (OAuth2) for granular, auditable authentication.

### Why 2-layer dbt instead of 3?

The INTERMEDIATE layer was initially planned but removed because no business logic was reused across multiple marts. The temporal join (ASOF JOIN for package-to-driver resolution) is used only by `fct_deliveries` and is integrated inline. Adding an intermediate layer without genuine reuse adds complexity without value.

### Why Kubernetes CronJob instead of Airflow?

For this project's pipeline (5 sequential tasks), a CronJob is simpler than deploying a full Airflow instance (4+ containers). The CronJob achieves the same result with a single container. Airflow (Astronomer) was implemented separately and validated (5 tasks, all green), demonstrating proficiency with both approaches.

### Why ASOF JOIN for driver resolution?

Delivery events contain `vehicle_id` but not `driver_id`. The driver-to-vehicle mapping is implicit via `driver_events`. The ASOF JOIN finds, for each delivery event, the most recent driver assignment to that vehicle before the delivery timestamp — the standard temporal join pattern in event-driven analytics.

### Multi-Stage Docker Build for Spark

The Spark container requires Java 11 (PySpark 3.3.4 compatibility) and Python 3.10. No single base image provides both, so a multi-stage build copies the JDK from `eclipse-temurin:11-jdk` into `python:3.10-slim`. All JARs (Kafka + Hadoop-Azure) are downloaded at build time to avoid runtime Maven failures in network-restricted environments.

### Dual-Write Pattern

Spark writes Parquet to both local filesystem (debugging) and ADLS Gen2 (cloud). Controlled by `SPARK_WRITE_LOCAL` and `SPARK_WRITE_ADLS` environment variables. In Kubernetes, local write is disabled since there is no persistent local storage.

---

## 💰 Cost Management

| Resource | Cost | Notes |
|---|---|---|
| Azure ADLS Gen2 | ~$0.07 CAD/month | Student credits ($141 CAD available) |
| Snowflake | Trial ($400 USD) | ~119 days remaining |
| Docker Desktop K8s | Free | Local single-node cluster |
| Power BI Desktop | Free | Windows only |
| Kafka, Redis, Spark | Free | Open-source, containerized |

---

## 🔒 Security Practices

- **Two SAS tokens** with least-privilege separation (read for Snowflake, write for Python)
- **Kubernetes Secrets** for credential management (base64-encoded, gitignored)
- **dbt profiles with `env_var()`** — no hardcoded passwords in config files
- **`.gitignore`** covers `.env`, `terraform.tfvars`, `tfstate`, `secrets.yaml`, `profiles.yml`
- **SharedKey auth** scoped to the Spark container only

> **Production upgrade**: Azure AD Service Principal + Snowflake Storage Integration (requires tenant admin access, unavailable with student accounts).

---

## Auteur

**Khalifa Ababacar Seck**
Data Engineer


## 📄 Licence

MIT — voir [LICENSE](LICENSE)
