"""
Sink Redis : écrit l'état courant de chaque véhicule.

Pour chaque événement GPS nettoyé, on met à jour dans Redis l'état
temps réel du véhicule : dernière position, vitesse, statut, horodatage.
C'est ce que l'API temps réel (Brique 5) lira pour répondre à
« où est le véhicule ? ».

Écriture via foreachBatch : chaque micro-batch Spark est collecté côté
driver, puis poussé dans Redis avec un pipeline (écritures groupées).

Modèle de clé :
    vehicle:{vehicle_id}:state  -> HASH {lat, lon, speed_kmh, event_time, updated_at}
avec un TTL (REDIS_TTL_SECONDS) pour purger les véhicules inactifs.
"""
import os

import redis

# Connexion Redis lue depuis l'environnement (.env), avec défauts dev local.
_REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
_REDIS_DB = int(os.getenv("REDIS_DB", "0"))
_REDIS_TTL = int(os.getenv("REDIS_TTL_SECONDS", "86400"))


def _get_redis_client() -> redis.Redis:
    """
    Crée un client Redis. Appelé DANS foreachBatch (côté driver),
    donc une connexion par micro-batch — simple et suffisant ici.
    decode_responses=True : lit/écrit des str plutôt que des bytes.
    """
    return redis.Redis(
        host=_REDIS_HOST,
        port=_REDIS_PORT,
        db=_REDIS_DB,
        decode_responses=True,
    )


def write_gps_batch(batch_df, batch_id: int) -> None:
    """
    Fonction foreachBatch pour les événements GPS nettoyés.

    Pour chaque ligne du micro-batch, écrit/écrase l'état du véhicule
    dans Redis. On utilise un pipeline pour grouper les écritures
    (une seule aller-retour réseau au lieu d'une par véhicule).

    NB : au sein d'un micro-batch, si un véhicule a plusieurs positions,
    la dernière écrite l'emporte. Pour un état "courant", c'est acceptable ;
    on affinera (garder la plus récente par event_time) si besoin.
    """
    if batch_df.isEmpty():
        return

    # Colonnes minimales nécessaires à l'état courant.
    rows = (
        batch_df
        .select("vehicle_id", "driver_id", "lat", "lon", "speed_kmh", "event_time")
        .collect()
    )

    client = _get_redis_client()
    pipe = client.pipeline()

    for row in rows:
        key = f"vehicle:{row['vehicle_id']}:state"
        # event_time est un datetime Spark -> on le sérialise en ISO.
        event_time = row["event_time"].isoformat() if row["event_time"] else ""
        mapping = {
            "driver_id": row["driver_id"] or "",
            "lat": str(row["lat"]) if row["lat"] is not None else "",
            "lon": str(row["lon"]) if row["lon"] is not None else "",
            "speed_kmh": str(row["speed_kmh"]) if row["speed_kmh"] is not None else "",
            "event_time": event_time,
        }
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, _REDIS_TTL)  # TTL pour purger les véhicules inactifs

    pipe.execute()
    print(f"[redis_sink] batch {batch_id}: {len(rows)} véhicule(s) mis à jour dans Redis.")