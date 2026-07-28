"""
Structures de données du simulateur.
Chaque dataclass représente une entité métier ou un événement envoyé à Kafka.
La méthode to_dict() sérialise l'objet en dictionnaire prêt pour l'encodage JSON.
"""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
import uuid


def _now_iso() -> str:
    """Horodatage courant en ISO 8601 UTC (avec suffixe Z)."""
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    """Génère un identifiant court et lisible, ex. 'pkg-3f9a2b1c'."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ------------------------------------------------------------
# Énumérations de statuts
# ------------------------------------------------------------

class DriverStatus(str, Enum):
    """Statut d'un chauffeur au cours de son quart."""
    OFFLINE = "offline"
    ONLINE = "online"
    ON_BREAK = "on_break"
    DELIVERING = "delivering"


class DeliveryStatus(str, Enum):
    """Étapes du cycle de vie d'un colis."""
    CREATED = "created"
    PICKED_UP = "picked_up"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    FAILED = "failed"


# ------------------------------------------------------------
# Entités persistantes (état du simulateur)
# ------------------------------------------------------------

@dataclass
class Driver:
    """Un chauffeur, associé à un véhicule."""
    driver_id: str
    vehicle_id: str
    status: DriverStatus
    lat: float
    lon: float

    @staticmethod
    def create(lat: float, lon: float) -> "Driver":
        """Fabrique un chauffeur neuf, hors ligne, à une position donnée."""
        return Driver(
            driver_id=new_id("drv"),
            vehicle_id=new_id("veh"),
            status=DriverStatus.OFFLINE,
            lat=lat,
            lon=lon,
        )


@dataclass
class Order:
    """Une commande passée par un client, à livrer."""
    order_id: str
    package_id: str
    created_at: str
    dest_lat: float
    dest_lon: float
    status: DeliveryStatus

    @staticmethod
    def create(dest_lat: float, dest_lon: float) -> "Order":
        """Fabrique une commande neuve avec son colis associé."""
        return Order(
            order_id=new_id("ord"),
            package_id=new_id("pkg"),
            created_at=_now_iso(),
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            status=DeliveryStatus.CREATED,
        )


# ------------------------------------------------------------
# Événements (messages envoyés à Kafka)
# ------------------------------------------------------------

@dataclass
class GpsEvent:
    """Position GPS instantanée d'un véhicule. Topic: gps_positions (clé = vehicle_id)."""
    vehicle_id: str
    driver_id: str
    lat: float
    lon: float
    speed_kmh: float
    timestamp: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DeliveryEvent:
    """Changement de statut d'un colis. Topic: delivery_events (clé = package_id)."""
    package_id: str
    order_id: str
    vehicle_id: str
    status: str          # valeur de DeliveryStatus
    lat: float
    lon: float
    timestamp: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class OrderEvent:
    """Création/mise à jour d'une commande. Topic: orders (clé = package_id)."""
    order_id: str
    package_id: str
    dest_lat: float
    dest_lon: float
    status: str          # valeur de DeliveryStatus
    timestamp: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DriverEvent:
    """Changement de statut d'un chauffeur. Topic: driver_events (clé = driver_id)."""
    driver_id: str
    vehicle_id: str
    status: str          # valeur de DriverStatus
    lat: float
    lon: float
    timestamp: str

    def to_dict(self) -> dict:
        return asdict(self)