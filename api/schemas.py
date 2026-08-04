"""
Schémas Pydantic des réponses de l'API.

Définissent la structure et les types des réponses JSON. FastAPI les
utilise pour valider, sérialiser et documenter automatiquement l'API
(la doc interactive /docs est générée à partir de ces modèles).

Note : Redis stocke tout en chaînes (HASH). Ces schémas convertissent
les champs numériques en float/int au passage, pour une réponse propre.
"""
from typing import Optional

from pydantic import BaseModel, Field


class VehicleState(BaseModel):
    """État courant d'un véhicule, tel que lu dans Redis."""
    vehicle_id: str = Field(..., description="Identifiant du véhicule")
    driver_id: Optional[str] = Field(None, description="Chauffeur associé")
    lat: Optional[float] = Field(None, description="Latitude courante")
    lon: Optional[float] = Field(None, description="Longitude courante")
    speed_kmh: Optional[float] = Field(None, description="Vitesse (km/h)")
    event_time: Optional[str] = Field(None, description="Horodatage de la position")

    @staticmethod
    def from_redis(vehicle_id: str, state: dict) -> "VehicleState":
        """
        Construit un VehicleState à partir d'un dict Redis (valeurs en str).
        Convertit les champs numériques, en tolérant les valeurs manquantes.
        """
        def _to_float(value: Optional[str]) -> Optional[float]:
            if value is None or value == "":
                return None
            try:
                return float(value)
            except ValueError:
                return None

        return VehicleState(
            vehicle_id=vehicle_id,
            driver_id=state.get("driver_id") or None,
            lat=_to_float(state.get("lat")),
            lon=_to_float(state.get("lon")),
            speed_kmh=_to_float(state.get("speed_kmh")),
            event_time=state.get("event_time") or None,
        )


class VehicleList(BaseModel):
    """Liste des véhicules actuellement suivis."""
    count: int = Field(..., description="Nombre de véhicules suivis")
    vehicle_ids: list[str] = Field(..., description="Identifiants des véhicules")


class HealthStatus(BaseModel):
    """État de santé de l'API et de ses dépendances."""
    status: str = Field(..., description="'ok' si tout va bien, 'degraded' sinon")
    redis_connected: bool = Field(..., description="Redis répond-il ?")
    vehicles_tracked: int = Field(..., description="Véhicules actuellement en état")


class ErrorResponse(BaseModel):
    """Réponse d'erreur standardisée."""
    detail: str = Field(..., description="Message décrivant l'erreur")