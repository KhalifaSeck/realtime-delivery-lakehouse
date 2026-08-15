"""
DAG : delivery_pipeline_realtime

Orchestration du pipeline analytique, toutes les 5 min :
  1. copy_to_snowflake : charge les Parquet ADLS -> Snowflake RAW
  2. dbt_run_staging   : construit les 4 vues STAGING
  3. dbt_test_staging  : valide les tests STAGING
  4. dbt_run_marts     : construit les 5 tables MARTS
  5. dbt_test_marts    : valide les tests MARTS

Note : l'upload lake -> ADLS n'est plus nécessaire car Spark écrit
directement dans ADLS Gen2 en temps réel (dual-write via SharedKey).

Schedule : toutes les 5 min
Owner    : data-engineering
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ============================================================
# Configuration du DAG
# ============================================================
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Chemins dbt dans le conteneur Airflow
DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/delivery_dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt/profiles"


# ============================================================
# Fonctions Python
# ============================================================
def _copy_to_snowflake(**context):
    """
    Charge les nouveaux Parquet du stage ADLS vers Snowflake RAW.
    Spark écrit en continu dans ADLS ; cette task ingère les fichiers
    non encore chargés via COPY INTO (Snowflake track les fichiers
    déjà traités pendant 64 jours).
    """
    import sys
    sys.path.insert(0, "/usr/local/airflow/include")
    from ingestion.copy_to_snowflake import load_events

    results = load_events(truncate=False)
    total_loaded = sum(r["n_loaded"] for r in results)
    print(f"[copy_to_snowflake] Total lignes chargées : {total_loaded:,}")
    return {
        "total_rows": total_loaded,
        "per_table": {r["table"]: r["n_loaded"] for r in results}
    }


# ============================================================
# Définition du DAG
# ============================================================
with DAG(
    dag_id="delivery_pipeline_realtime",
    description="Pipeline analytique automatisé : ADLS -> Snowflake -> dbt (toutes les 5 min)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 8, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["delivery", "analytics", "realtime"],
) as dag:

    # ---------- Task 1 : COPY INTO Snowflake ----------
    copy_to_snowflake = PythonOperator(
        task_id="copy_to_snowflake",
        python_callable=_copy_to_snowflake,
    )

    # ---------- Task 2 : dbt run staging ----------
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select staging"
        ),
    )

    # ---------- Task 3 : dbt test staging ----------
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select staging"
        ),
    )

    # ---------- Task 4 : dbt run marts ----------
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts"
        ),
    )

    # ---------- Task 5 : dbt test marts ----------
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select marts"
        ),
    )

    # Orchestration
    (
        copy_to_snowflake
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
    )