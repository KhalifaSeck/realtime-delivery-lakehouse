"""
Configuration centrale du simulateur.
Charge les variables depuis .env (avec valeurs par défaut pour le dev local).
"""
import os
from dataclasses import dataclass, field


def _get_env(key: str, default: str) -> str:
    """Lit une variable d'environnement, avec valeur par défaut."""
    return os.environ.get(key, default)


def _get_env_int(key: str, default: int) -> int:
    """Lit une variable d'environnement entière, avec valeur par défaut."""
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class KafkaConfig:
    """Paramètres de connexion Kafka et noms des topics."""
    bootstrap_servers: str = field(
        default_factory=lambda: _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_gps: str = field(
        default_factory=lambda: _get_env("KAFKA_TOPIC_GPS", "gps_positions")
    )
    topic_delivery: str = field(
        default_factory=lambda: _get_env("KAFKA_TOPIC_DELIVERY", "delivery_events")
    )
    topic_orders: str = field(
        default_factory=lambda: _get_env("KAFKA_TOPIC_ORDERS", "orders")
    )
    topic_drivers: str = field(
        default_factory=lambda: _get_env("KAFKA_TOPIC_DRIVERS", "driver_events")
    )


@dataclass(frozen=True)
class SimulatorConfig:
    """Paramètres de comportement du simulateur."""
    nb_drivers: int = field(
        default_factory=lambda: _get_env_int("SIM_NB_DRIVERS", 20)
    )
    tick_seconds: float = field(
        default_factory=lambda: float(_get_env("SIM_TICK_SECONDS", "2"))
    )
    city: str = field(
        default_factory=lambda: _get_env("SIM_CITY", "Montreal")
    )

    # Bornes géographiques approximatives de l'île de Montréal
    # (utilisées par le générateur GPS à l'étape suivante)
    lat_min: float = 45.40
    lat_max: float = 45.70
    lon_min: float = -73.95
    lon_max: float = -73.47


@dataclass(frozen=True)
class Config:
    """Configuration racine, regroupe toutes les sous-configs."""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    simulator: SimulatorConfig = field(default_factory=SimulatorConfig)


# Instance unique importable partout : from simulator.config import config
config = Config()