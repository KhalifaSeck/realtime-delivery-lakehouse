FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    snowflake-connector-python \
    dbt-core \
    dbt-snowflake \
    azure-storage-file-datalake \
    python-dotenv

# Code d'ingestion
COPY ingestion/ /app/ingestion/

# Projet dbt
COPY delivery_dbt/ /app/dbt/delivery_dbt/

# Profiles dbt (avec env_var pour les credentials)
COPY pipeline/dbt_profiles/profiles.yml /app/dbt/profiles/profiles.yml

# Script d'orchestration
COPY pipeline/run_pipeline.sh /app/run_pipeline.sh
RUN chmod +x /app/run_pipeline.sh

CMD ["/app/run_pipeline.sh"]