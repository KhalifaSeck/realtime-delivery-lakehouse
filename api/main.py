"""
Application FastAPI de suivi de livraisons en temps réel.

Instrumentée pour Prometheus : expose /metrics avec :
  - métriques HTTP standards (via prometheus-fastapi-instrumentator) ;
  - métriques métier custom (via api.metrics.refresh_metrics).

Lancement : uvicorn api.main:app --reload
Doc interactive : http://localhost:8000/docs
Métriques Prometheus : http://localhost:8000/metrics
"""
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from api.routers import vehicles, health
from api.metrics import refresh_metrics

app = FastAPI(
    title="Realtime Delivery API",
    description="Suivi temps réel des véhicules de livraison (lecture Redis).",
    version="0.1.0",
)

# --- Instrumentation Prometheus ---
# L'Instrumentator branche automatiquement :
#  - un endpoint /metrics ;
#  - des métriques HTTP par endpoint (compteur, latence, statuts) ;
#  - au format text Prometheus, prêt à être scrapé.
instrumentator = Instrumentator(
    should_group_status_codes=False,   # 200, 404, 500 séparément (utile en debug)
    should_ignore_untemplated=True,    # ignore les 404 sur URLs random
    excluded_handlers=["/metrics"],    # ne s'auto-scrape pas
)
instrumentator.instrument(app).expose(app, endpoint="/metrics", tags=["monitoring"])


# --- Métriques métier : rafraîchies avant chaque scrape ---
# add_middleware n'est pas idéal ici (appelée à chaque requête).
# La bonne approche : refresh_metrics() est déclenché quand /metrics est appelé.
@app.middleware("http")
async def refresh_business_metrics(request, call_next):
    """Rafraîchit les métriques métier juste avant de servir /metrics."""
    if request.url.path == "/metrics":
        refresh_metrics()
    return await call_next(request)


# --- Routers ---
app.include_router(health.router)
app.include_router(vehicles.router)


@app.get("/", tags=["root"])
def root() -> dict:
    """Point d'entrée : liste les endpoints principaux."""
    return {
        "service": "Realtime Delivery API",
        "docs": "/docs",
        "endpoints": ["/health", "/vehicles", "/vehicles/{vehicle_id}", "/metrics"],
    }