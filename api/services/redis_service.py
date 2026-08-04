"""
Service de lecture de l'état temps réel dans Redis.

L'API ne calcule rien : elle lit l'état courant des véhicules que le
pipeline Spark maintient à jour (sink Redis, clés vehicle:{id}:state).
Ce service encapsule toutes les lectures Redis pour que les routers
n'aient pas à connaître les détails des clés.
"""
import os
from typing import Optional

import redis


class RedisService:
    """
    Accès en lecture à l'état des véhicules dans Redis.

    Connexion lue depuis l'environnement (mêmes variables que le sink),
    avec valeurs par défaut dev local.
    """

    def __init__(self) -> None:
        self._client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
        )

    def ping(self) -> bool:
        """Vérifie que Redis répond (utilisé par le health check)."""
        try:
            return self._client.ping()
        except redis.RedisError:
            return False

    def get_vehicle_state(self, vehicle_id: str) -> Optional[dict]:
        """
        Récupère l'état courant d'un véhicule.

        Retourne le HASH Redis (lat, lon, speed_kmh, driver_id, event_time)
        sous forme de dict, ou None si le véhicule n'existe pas dans Redis
        (jamais vu, ou état expiré par le TTL).
        """
        key = f"vehicle:{vehicle_id}:state"
        state = self._client.hgetall(key)
        return state if state else None

    def list_vehicle_ids(self) -> list[str]:
        """
        Liste les identifiants de tous les véhicules ayant un état courant.

        Parcourt les clés vehicle:*:state avec SCAN (non bloquant, sûr en
        production, contrairement à KEYS). Extrait le vehicle_id de chaque clé.
        """
        ids = []
        for key in self._client.scan_iter(match="vehicle:*:state"):
            # clé = "vehicle:{id}:state" -> on extrait le milieu.
            parts = key.split(":")
            if len(parts) == 3:
                ids.append(parts[1])
        return ids

    def count_vehicles(self) -> int:
        """Compte les véhicules actuellement suivis (pour le health/stats)."""
        return len(self.list_vehicle_ids())


# Instance unique, importable par les routers.
redis_service = RedisService()