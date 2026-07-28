"""
Producteur d'événements métier.
Route chaque type d'événement du simulateur vers son topic Kafka,
avec la bonne clé de partition, en s'appuyant sur BaseProducer.

Mapping topic / clé de partition (cf. README) :
    GpsEvent      -> gps_positions   (clé = vehicle_id)
    DeliveryEvent -> delivery_events (clé = package_id)
    OrderEvent    -> orders          (clé = package_id)
    DriverEvent   -> driver_events   (clé = driver_id)
"""
import logging

from simulator.config import config
from simulator.models import (
    GpsEvent,
    DeliveryEvent,
    OrderEvent,
    DriverEvent,
)
from simulator.producers.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class EventProducer:
    """
    Producteur de haut niveau, orienté événements métier.

    Expose une méthode publish() par type d'événement. Chacune connaît
    son topic et sa clé de partition : l'appelant (la boucle du simulateur)
    n'a pas à s'en soucier, il publie juste ses événements.
    """

    def __init__(self, producer: BaseProducer | None = None) -> None:
        """
        `producer` : injectable pour les tests (on peut passer un faux
        BaseProducer). Par défaut, on en crée un vrai connecté à Kafka.
        """
        self._producer = producer or BaseProducer()
        self._kafka = config.kafka

    # ---- GPS ----------------------------------------------------------
    def publish_gps(self, event: GpsEvent) -> None:
        """Publie une position GPS sur gps_positions, clé = vehicle_id."""
        self._producer.send(
            topic=self._kafka.topic_gps,
            value=event.to_dict(),
            key=event.vehicle_id,
        )

    # ---- Livraison ----------------------------------------------------
    def publish_delivery(self, event: DeliveryEvent) -> None:
        """Publie un événement de livraison sur delivery_events, clé = package_id."""
        self._producer.send(
            topic=self._kafka.topic_delivery,
            value=event.to_dict(),
            key=event.package_id,
        )

    # ---- Commande -----------------------------------------------------
    def publish_order(self, event: OrderEvent) -> None:
        """Publie un événement de commande sur orders, clé = package_id."""
        self._producer.send(
            topic=self._kafka.topic_orders,
            value=event.to_dict(),
            key=event.package_id,
        )

    # ---- Chauffeur ----------------------------------------------------
    def publish_driver(self, event: DriverEvent) -> None:
        """Publie un événement de chauffeur sur driver_events, clé = driver_id."""
        self._producer.send(
            topic=self._kafka.topic_drivers,
            value=event.to_dict(),
            key=event.driver_id,
        )

    # ---- Cycle de vie -------------------------------------------------
    def flush(self) -> None:
        """Force l'envoi des messages en attente."""
        self._producer.flush()

    def close(self) -> None:
        """Ferme proprement le producteur sous-jacent."""
        self._producer.close()