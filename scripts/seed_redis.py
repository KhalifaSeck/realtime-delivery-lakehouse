"""
Peuple Redis avec des véhicules de test.

Permet de tester l'API sans lancer tout le pipeline (Kafka + Spark +
simulateur). Écrit quelques états de véhicules directement dans Redis,
au même format que le sink Spark (vehicle:{id}:state).

Usage :
    python -m scripts.seed_redis           # ajoute 5 véhicules de test
    python -m scripts.seed_redis --clear   # vide d'abord les véhicules de test

Prérequis : conteneur Redis démarré.
"""
import argparse
import os
import random
from datetime import datetime, timezone

import redis


# Bornes Montréal (cohérentes avec le simulateur).
_LAT_MIN, _LAT_MAX = 45.40, 45.70
_LON_MIN, _LON_MAX = -73.95, -73.47

# Préfixe des véhicules de test, pour les distinguer/nettoyer facilement.
_TEST_PREFIX = "veh-test-"

_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "86400"))


def _client() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )


def clear_test_vehicles(client: redis.Redis) -> int:
    """Supprime tous les véhicules de test (clés vehicle:veh-test-*:state)."""
    removed = 0
    for key in client.scan_iter(match=f"vehicle:{_TEST_PREFIX}*:state"):
        client.delete(key)
        removed += 1
    return removed


def seed_vehicles(client: redis.Redis, n: int = 5) -> list[str]:
    """
    Écrit n véhicules de test dans Redis, au format du sink Spark.
    Retourne la liste des IDs créés.
    """
    created = []
    for i in range(n):
        vehicle_id = f"{_TEST_PREFIX}{i:03d}"
        key = f"vehicle:{vehicle_id}:state"
        mapping = {
            "driver_id": f"drv-test-{i:03d}",
            "lat": str(round(random.uniform(_LAT_MIN, _LAT_MAX), 6)),
            "lon": str(round(random.uniform(_LON_MIN, _LON_MAX), 6)),
            "speed_kmh": str(round(random.uniform(0, 50), 1)),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        client.hset(key, mapping=mapping)
        client.expire(key, _TTL_SECONDS)
        created.append(vehicle_id)
    return created


def main():
    parser = argparse.ArgumentParser(description="Peuple Redis avec des véhicules de test.")
    parser.add_argument("--clear", action="store_true",
                        help="Vide les véhicules de test avant d'en créer.")
    parser.add_argument("-n", type=int, default=5,
                        help="Nombre de véhicules de test à créer (défaut 5).")
    args = parser.parse_args()

    client = _client()
    if not client.ping():
        print("Redis ne répond pas. Démarre le conteneur (docker compose up -d).")
        return

    if args.clear:
        removed = clear_test_vehicles(client)
        print(f"{removed} véhicule(s) de test supprimé(s).")

    created = seed_vehicles(client, args.n)
    print(f"{len(created)} véhicule(s) de test créé(s) :")
    for vid in created:
        print(f"  - {vid}")
    print("\nTeste l'API : GET /vehicles puis GET /vehicles/{id}")


if __name__ == "__main__":
    main()