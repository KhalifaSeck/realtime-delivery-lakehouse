"""
Générateur d'événements de livraison.
Fait progresser un colis dans son cycle de vie et produit le DeliveryEvent
correspondant à chaque transition de statut.

Cycle de vie normal :
    created -> picked_up -> in_transit -> delivered

Une petite proportion de livraisons échoue (failed) depuis in_transit,
pour donner de la matière à la détection d'anomalies (Brique 3).

Ce générateur ne décide PAS quand une transition a lieu (c'est le rôle
de la boucle principale, selon la position GPS et l'arrivée à destination).
Il fournit les transitions valides et fabrique l'événement associé.
"""
import random

from simulator.models import (
    Order,
    DeliveryEvent,
    DeliveryStatus,
    _now_iso,
)


# Transitions autorisées : à chaque statut, l'étape normale suivante.
_NEXT_STATUS = {
    DeliveryStatus.CREATED: DeliveryStatus.PICKED_UP,
    DeliveryStatus.PICKED_UP: DeliveryStatus.IN_TRANSIT,
    DeliveryStatus.IN_TRANSIT: DeliveryStatus.DELIVERED,
}

# Probabilité qu'une livraison en transit échoue au lieu d'être livrée.
_FAILURE_PROBABILITY = 0.08


def next_status(current: DeliveryStatus) -> DeliveryStatus | None:
    """
    Donne le statut suivant dans le cycle de vie normal.

    Depuis IN_TRANSIT, la livraison peut échouer (FAILED) avec une petite
    probabilité, au lieu de passer à DELIVERED.

    Retourne None si le statut est terminal (DELIVERED ou FAILED) :
    plus aucune transition possible.
    """
    if current in (DeliveryStatus.DELIVERED, DeliveryStatus.FAILED):
        return None

    if current == DeliveryStatus.IN_TRANSIT:
        if random.random() < _FAILURE_PROBABILITY:
            return DeliveryStatus.FAILED
        return DeliveryStatus.DELIVERED

    return _NEXT_STATUS[current]


def advance_delivery(
    order: Order, vehicle_id: str, lat: float, lon: float
) -> DeliveryEvent | None:
    """
    Fait avancer le colis d'un cran dans son cycle de vie.

    - Calcule le statut suivant à partir du statut courant de la commande.
    - Met à jour `order.status` en place (l'état avance).
    - Fabrique le DeliveryEvent à publier sur le topic 'delivery_events',
      horodaté et géolocalisé à la position courante du véhicule.

    Retourne None si le colis est déjà dans un statut terminal
    (rien à publier).
    """
    new_status = next_status(order.status)
    if new_status is None:
        return None

    # L'état de la commande avance.
    order.status = new_status

    return DeliveryEvent(
        package_id=order.package_id,
        order_id=order.order_id,
        vehicle_id=vehicle_id,
        status=new_status.value,
        lat=round(lat, 6),
        lon=round(lon, 6),
        timestamp=_now_iso(),
    )


def is_terminal(order: Order) -> bool:
    """Vrai si le colis a atteint un statut final (livré ou échoué)."""
    return order.status in (DeliveryStatus.DELIVERED, DeliveryStatus.FAILED)