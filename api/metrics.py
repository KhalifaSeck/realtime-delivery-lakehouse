"""
Métriques Prometheus custom exposées par l'API.

Trois familles de métriques métier :
  1. Redis (état courant)      : véhicules suivis, vitesse moyenne.
  2. Data Quality (GX)         : score global + par type d'événement.
  3. Lake (fraîcheur, comptages, anomalies, statuts) :
        - événements par type
        - âge du dernier événement par type
        - anomalies GPS par type d'anomalie
        - chauffeurs par statut (offline/online/on_break/delivering)

En plus, l'Instrumentator FastAPI fournit automatiquement :
  - http_requests_total (par endpoint, méthode, statut)
  - http_request_duration_seconds (histogramme, pour P95/P99)

Ces métriques sont rafraîchies à chaque scrape /metrics via un middleware.
"""
import glob
import os
from datetime import datetime, timezone

import pandas as pd
from prometheus_client import Gauge

from api.services.redis_service import redis_service


# --- Chemin du lake (mêmes valeurs par défaut que le sink) ---
_LAKE = os.getenv("LAKE_OUTPUT_DIR", r"C:\delivery-lake")
_EVENT_TYPES = ["gps", "delivery", "order", "driver"]
_ANOMALY_TYPES = ["SPEED_TOO_HIGH", "SPEED_MISMATCH", "IMMOBILE", "GPS_JUMP"]
_DRIVER_STATUSES = ["offline", "online", "on_break", "delivering"]
# Cache global pour GX (évite de recalculer à chaque scrape).
_gx_cache = {"last_run": 0, "report": None}
_GX_CACHE_TTL_SECONDS = 300

# ============================================================
# 1. MÉTRIQUES REDIS (état courant, "hot")
# ============================================================
vehicles_tracked = Gauge(
    "delivery_vehicles_tracked",
    "Véhicules avec état courant dans Redis.",
)

vehicles_avg_speed_kmh = Gauge(
    "delivery_vehicles_avg_speed_kmh",
    "Vitesse moyenne des véhicules actuellement suivis (Redis).",
)


# ============================================================
# 2. MÉTRIQUES DATA QUALITY (Great Expectations)
# ============================================================
data_quality_score = Gauge(
    "delivery_data_quality_score",
    "Score global de qualité (0-100), tous types confondus.",
)

data_quality_expectations_total = Gauge(
    "delivery_data_quality_expectations_total",
    "Nombre total d'attentes GX évaluées.",
)

data_quality_expectations_passed = Gauge(
    "delivery_data_quality_expectations_passed",
    "Nombre d'attentes GX réussies.",
)

data_quality_score_by_type = Gauge(
    "delivery_data_quality_score_by_type",
    "Score de qualité par type d'événement (0-100).",
    ["event_type"],
)


# ============================================================
# 3. MÉTRIQUES LAKE (historique, comptage, anomalies, statuts)
# ============================================================
lake_events_total = Gauge(
    "delivery_lake_events_total",
    "Nombre d'événements dans le lake, par type.",
    ["event_type"],
)

lake_last_event_age_seconds = Gauge(
    "delivery_lake_last_event_age_seconds",
    "Âge en secondes du dernier événement du lake, par type (fraîcheur).",
    ["event_type"],
)

lake_anomalies_total = Gauge(
    "delivery_lake_anomalies_total",
    "Nombre d'anomalies détectées dans le lake GPS, par type d'anomalie.",
    ["anomaly_type"],
)

drivers_by_status = Gauge(
    "delivery_drivers_by_status",
    "Nombre de chauffeurs par statut (dernier statut connu).",
    ["status"],
)


# ============================================================
# Fonctions de rafraîchissement
# ============================================================
def _refresh_redis_metrics() -> None:
    """Compte les véhicules et calcule leur vitesse moyenne."""
    try:
        ids = redis_service.list_vehicle_ids()
        vehicles_tracked.set(len(ids))

        # Vitesse moyenne (skippe les valeurs manquantes).
        speeds = []
        for vid in ids:
            state = redis_service.get_vehicle_state(vid)
            if state and state.get("speed_kmh"):
                try:
                    speeds.append(float(state["speed_kmh"]))
                except ValueError:
                    pass
        vehicles_avg_speed_kmh.set(sum(speeds) / len(speeds) if speeds else 0)
    except Exception as e:
        print(f"[metrics] Erreur Redis: {e}")
        vehicles_tracked.set(0)
        vehicles_avg_speed_kmh.set(0)

def _refresh_quality_metrics() -> None:
    """
    Score GX global et par type d'événement.
    Utilise un cache pour ne recalculer que toutes les 60s
    (GX est lent, ~4s, et le score varie peu à haute fréquence).
    """
    import time
    now_ts = time.time()

    # Cache TTL 60s : on recalcule seulement si le cache est périmé.
    if now_ts - _gx_cache["last_run"] < _GX_CACHE_TTL_SECONDS and _gx_cache["report"] is not None:
        report = _gx_cache["report"]
    else:
        try:
            from quality.validate_lake import validate_all
            report = validate_all()
            _gx_cache["report"] = report
            _gx_cache["last_run"] = now_ts
        except Exception as e:
            print(f"[metrics] Erreur GX: {e}")
            data_quality_score.set(0)
            data_quality_expectations_total.set(0)
            data_quality_expectations_passed.set(0)
            return

    # Application des valeurs.
    try:
        data_quality_score.set(float(report.get("quality_score", 0)))
        data_quality_expectations_total.set(float(report.get("total_expectations", 0)))
        data_quality_expectations_passed.set(float(report.get("total_success", 0)))

        for event_type, summary in report.get("by_type", {}).items():
            if summary.get("status") == "no_data":
                data_quality_score_by_type.labels(event_type=event_type).set(0)
                continue
            n_total = summary["n_expectations"]
            n_ok = summary["n_success"]
            score = 100.0 * n_ok / n_total if n_total else 0
            data_quality_score_by_type.labels(event_type=event_type).set(score)
    except Exception as e:
        print(f"[metrics] Erreur GX application: {e}")

def _refresh_lake_metrics() -> None:
    """Comptage, fraîcheur, anomalies, statuts chauffeurs (lecture pandas des Parquet)."""
    now = datetime.now(timezone.utc)

    # Initialise les anomalies à 0 (écrasées ensuite si trouvées).
    for atype in _ANOMALY_TYPES:
        lake_anomalies_total.labels(anomaly_type=atype).set(0)

    # Initialise les statuts chauffeurs à 0.
    for status in _DRIVER_STATUSES:
        drivers_by_status.labels(status=status).set(0)

    # --- Comptage, fraîcheur, anomalies par type d'événement ---
    for event_type in _EVENT_TYPES:
        path = os.path.join(_LAKE, "events", event_type)
        files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)

        if not files:
            lake_events_total.labels(event_type=event_type).set(0)
            lake_last_event_age_seconds.labels(event_type=event_type).set(-1)
            continue

        try:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            lake_events_total.labels(event_type=event_type).set(len(df))

            # Fraîcheur : âge du dernier event_time.
            if "event_time" in df.columns and len(df):
                latest = pd.to_datetime(df["event_time"], utc=True, errors="coerce").max()
                if pd.notna(latest):
                    age = (now - latest.to_pydatetime()).total_seconds()
                    lake_last_event_age_seconds.labels(event_type=event_type).set(age)

            # Anomalies : uniquement dans le flux GPS.
            if event_type == "gps" and "anomaly_types" in df.columns:
                exploded = df["anomaly_types"].dropna().explode().dropna()
                counts = exploded.value_counts().to_dict()
                for atype in _ANOMALY_TYPES:
                    lake_anomalies_total.labels(anomaly_type=atype).set(counts.get(atype, 0))
        except Exception as e:
            print(f"[metrics] Erreur lake ({event_type}): {e}")

    # --- Statuts chauffeurs : dernier statut connu par driver_id ---
    try:
        driver_path = os.path.join(_LAKE, "events", "driver")
        files = glob.glob(os.path.join(driver_path, "**", "*.parquet"), recursive=True)

        if files:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            if "driver_id" in df.columns and "status" in df.columns and "event_time" in df.columns:
                df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
                # Dernière ligne par chauffeur = son statut actuel.
                latest = df.sort_values("event_time").groupby("driver_id").tail(1)
                counts = latest["status"].value_counts().to_dict()
                for status, n in counts.items():
                    if status in _DRIVER_STATUSES:
                        drivers_by_status.labels(status=status).set(n)
    except Exception as e:
        print(f"[metrics] Erreur drivers status: {e}")


def refresh_metrics() -> None:
    """
    Rafraîchit toutes les métriques métier. Appelée à chaque scrape /metrics.
    Chaque famille est indépendante : une erreur dans l'une n'affecte pas les autres.
    """
    _refresh_redis_metrics()
    _refresh_quality_metrics()
    _refresh_lake_metrics()