"""
Charge les Parquet du stage ADLS Gen2 dans les tables RAW Snowflake.

Lance un COPY INTO par type d'événement, en utilisant MATCH_BY_COLUMN_NAME
pour mapper automatiquement les colonnes Parquet aux colonnes de table.

Usage :
    python -m ingestion.copy_to_snowflake                  # tous les types
    python -m ingestion.copy_to_snowflake --type gps       # un seul type
    python -m ingestion.copy_to_snowflake --truncate       # vide les tables avant

Auth : credentials depuis .env ou variables d'environnement.
"""
import argparse
import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# Configuration
# ============================================================
_SNOW_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
_SNOW_USER = os.getenv("SNOWFLAKE_USER")
_SNOW_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
_SNOW_ROLE = os.getenv("SNOWFLAKE_ROLE", "DELIVERY_ROLE")
_SNOW_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DELIVERY_WH")
_SNOW_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DELIVERY_LAKEHOUSE")
_SNOW_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

# Mapping type d'événement -> nom de la table cible et préfixe du chemin ADLS.
_EVENT_TO_TABLE = {
    "gps":      ("GPS_EVENTS",      "gps/"),
    "delivery": ("DELIVERY_EVENTS", "delivery/"),
    "order":    ("ORDERS",          "order/"),
    "driver":   ("DRIVER_EVENTS",   "driver/"),
}


def _get_connection():
    """Ouvre une connexion Snowflake authentifiée via .env."""
    return snowflake.connector.connect(
        account=_SNOW_ACCOUNT,
        user=_SNOW_USER,
        password=_SNOW_PASSWORD,
        role=_SNOW_ROLE,
        warehouse=_SNOW_WAREHOUSE,
        database=_SNOW_DATABASE,
        schema=_SNOW_SCHEMA,
    )


def _copy_into_table(cursor, event_type: str) -> dict:
    """
    Exécute le COPY INTO pour un type d'événement.

    Utilise MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE et PATTERN pour
    ne charger que les vrais fichiers Parquet (ignore _spark_metadata).
    """
    table_name, path_prefix = _EVENT_TO_TABLE[event_type]

    sql = f"""
        COPY INTO {_SNOW_SCHEMA}.{table_name}
        FROM @DELIVERY_STAGE/{path_prefix}
        FILE_FORMAT = (FORMAT_NAME = PARQUET_FORMAT)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
        PATTERN = '.*\\.parquet'
    """

    print(f"\n=== COPY INTO {table_name} ===")
    cursor.execute(sql)

    rows = cursor.fetchall()
    n_files = len(rows)

    # Parsing robuste : le format de retour varie selon la version
    # Snowflake et les options (PATTERN, FORCE, etc.).
    # Format standard : (file, status, rows_parsed, rows_loaded, error_limit, errors_seen, ...)
    # Format minimal (0 fichiers) : tuple court ou résultat vide.
    n_loaded = 0
    n_errors = 0
    for r in rows:
        try:
            if len(r) >= 6:
                n_loaded += (r[3] or 0)
                n_errors += (r[5] or 0)
            elif len(r) >= 4:
                n_loaded += (r[3] or 0)
            elif len(r) >= 2:
                if str(r[1]).upper() == 'LOADED':
                    n_loaded += 1
        except (IndexError, TypeError):
            pass

    print(f"  Fichiers traités : {n_files}")
    print(f"  Lignes chargées  : {n_loaded:,}")
    print(f"  Erreurs          : {n_errors}")

    return {
        "table": table_name,
        "n_files": n_files,
        "n_loaded": n_loaded,
        "n_errors": n_errors,
    }


def _truncate_table(cursor, event_type: str) -> None:
    """Vide la table avant de re-charger (utile en dev pour éviter les doublons)."""
    table_name, _ = _EVENT_TO_TABLE[event_type]
    print(f"  TRUNCATE {table_name}...")
    cursor.execute(f"TRUNCATE TABLE {_SNOW_SCHEMA}.{table_name}")


def _count_rows(cursor, event_type: str) -> int:
    """Compte les lignes actuelles de la table."""
    table_name, _ = _EVENT_TO_TABLE[event_type]
    cursor.execute(f"SELECT COUNT(*) FROM {_SNOW_SCHEMA}.{table_name}")
    return cursor.fetchone()[0]


def load_events(event_type=None, truncate=False):
    """Charge un ou tous les types depuis ADLS vers Snowflake."""
    types = [event_type] if event_type else list(_EVENT_TO_TABLE.keys())
    results = []

    with _get_connection() as conn:
        with conn.cursor() as cursor:
            for et in types:
                if truncate:
                    _truncate_table(cursor, et)
                summary = _copy_into_table(cursor, et)
                summary["n_rows_after"] = _count_rows(cursor, et)
                results.append(summary)

    return results


def main():
    parser = argparse.ArgumentParser(description="COPY INTO ADLS -> Snowflake RAW.")
    parser.add_argument("--type", choices=list(_EVENT_TO_TABLE.keys()),
                        help="Ne charger qu'un type.")
    parser.add_argument("--truncate", action="store_true",
                        help="Vider les tables avant de charger.")
    args = parser.parse_args()

    # Vérifications.
    if not _SNOW_ACCOUNT or not _SNOW_PASSWORD:
        print("ERREUR : .env incomplet (SNOWFLAKE_ACCOUNT ou SNOWFLAKE_PASSWORD manquant).")
        return 1

    print(f"Snowflake : {_SNOW_ACCOUNT}")
    print(f"Warehouse : {_SNOW_WAREHOUSE}")
    print(f"Schema    : {_SNOW_DATABASE}.{_SNOW_SCHEMA}")
    print(f"Type(s)   : {args.type or 'tous'}")
    print(f"Truncate  : {args.truncate}")

    results = load_events(event_type=args.type, truncate=args.truncate)

    # Résumé final.
    print("\n" + "=" * 60)
    print(f"{'Table':20} {'Fichiers':>10} {'Lignes chargées':>18} {'Total table':>15}")
    print("-" * 60)
    total_loaded = 0
    total_files = 0
    for r in results:
        print(f"{r['table']:20} {r['n_files']:>10,} {r['n_loaded']:>18,} {r['n_rows_after']:>15,}")
        total_loaded += r["n_loaded"]
        total_files += r["n_files"]
    print("-" * 60)
    print(f"{'TOTAL':20} {total_files:>10,} {total_loaded:>18,}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())