"""
Générateur de commandes.
Produit des commandes neuves avec une destination aléatoire située
dans les bornes géographiques de la ville (définies dans la config).

Une commande fraîchement créée donne lieu à :
  - une entité Order (état suivi par le simulateur),
  - un OrderEvent à publier sur le topic 'orders'.
"""
import random

from simulator.config import config
from simulator.models import Order, OrderEvent, _now_iso


def _random_destination() -> tuple[float, float]:
    """
    Tire une destination aléatoire uniforme dans le rectangle géographique
    de la ville (bornes lat/lon de la config du simulateur).
    Retourne (lat, lon).
    """
    sim = config.simulator
    lat = random.uniform(sim.lat_min, sim.lat_max)
    lon = random.uniform(sim.lon_min, sim.lon_max)
    return round(lat, 6), round(lon, 6)


def new_order() -> tuple[Order, OrderEvent]:
    """
    Crée une commande neuve avec une destination aléatoire en ville.

    Retourne un couple :
      - l'entité Order (à conserver dans l'état du simulateur),
      - l'OrderEvent correspondant (à publier sur Kafka).

    Les deux partagent les mêmes order_id / package_id : l'entité suit
    le cycle de vie, l'événement est l'instantané publié à l'instant t.
    """
    dest_lat, dest_lon = _random_destination()
    order = Order.create(dest_lat=dest_lat, dest_lon=dest_lon)

    event = OrderEvent(
        order_id=order.order_id,
        package_id=order.package_id,
        dest_lat=order.dest_lat,
        dest_lon=order.dest_lon,
        status=order.status.value,
        timestamp=order.created_at,
    )
    return order, event


def should_create_order(probability: float = 0.3) -> bool:
    """
    Décide aléatoirement si une nouvelle commande apparaît à ce tick.
    `probability` = chance de création par appel (0.0 à 1.0).
    Permet un flux de commandes irrégulier plutôt qu'une cadence fixe.
    """
    return random.random() < probability