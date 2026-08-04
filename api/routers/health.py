"""
Endpoint de santé de l'API.

    GET /health -> état de l'API et de ses dépendances (Redis).

Utilisé pour le monitoring et les probes (Kubernetes en Brique finale).
"""
from fastapi import APIRouter

from api.services.redis_service import redis_service
from api.schemas import HealthStatus

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthStatus)
def health() -> HealthStatus:
    """
    Vérifie l'état de l'API et de Redis.

    status = 'ok' si Redis répond, 'degraded' sinon.
    """
    redis_ok = redis_service.ping()
    vehicles = redis_service.count_vehicles() if redis_ok else 0

    return HealthStatus(
        status="ok" if redis_ok else "degraded",
        redis_connected=redis_ok,
        vehicles_tracked=vehicles,
    )