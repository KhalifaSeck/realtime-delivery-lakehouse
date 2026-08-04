"""
Endpoints de suivi des véhicules.

Expose l'état temps réel des véhicules lu dans Redis :
    GET /vehicles           -> liste des véhicules suivis
    GET /vehicles/{id}      -> état courant d'un véhicule

Les routers ne font que orchestrer : ils appellent le service Redis,
convertissent via les schémas Pydantic, et gèrent les cas d'absence (404).
"""
from fastapi import APIRouter, HTTPException

from api.services.redis_service import redis_service
from api.schemas import VehicleState, VehicleList

router = APIRouter(prefix="/vehicles", tags=["vehicles"])


@router.get("", response_model=VehicleList)
def list_vehicles() -> VehicleList:
    """
    Liste tous les véhicules ayant un état courant dans Redis.
    """
    ids = redis_service.list_vehicle_ids()
    return VehicleList(count=len(ids), vehicle_ids=ids)


@router.get(
    "/{vehicle_id}",
    response_model=VehicleState,
    responses={404: {"description": "Véhicule non trouvé"}},
)
def get_vehicle(vehicle_id: str) -> VehicleState:
    """
    Retourne l'état courant d'un véhicule (dernière position connue).

    404 si le véhicule n'est pas dans Redis (jamais vu, ou état expiré
    par le TTL).
    """
    state = redis_service.get_vehicle_state(vehicle_id)
    if state is None:
        raise HTTPException(
            status_code=404,
            detail=f"Véhicule '{vehicle_id}' introuvable ou état expiré.",
        )
    return VehicleState.from_redis(vehicle_id, state)