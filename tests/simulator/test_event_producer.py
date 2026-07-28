"""
Tests unitaires du producteur d'événements.
Vérifie que chaque type d'événement est routé vers le bon topic
avec la bonne clé de partition, sans connexion Kafka réelle.

On injecte un faux BaseProducer qui capture les appels send() au lieu
de les envoyer à Kafka. C'est le pattern d'injection de dépendance
prévu dans EventProducer.

Lancement : pytest tests/simulator/test_event_producer.py -v
"""
from simulator.config import config
from simulator.models import GpsEvent, DeliveryEvent, OrderEvent, DriverEvent
from simulator.producers.event_producer import EventProducer


class FakeBaseProducer:
    """
    Faux producteur : enregistre chaque appel send() dans une liste
    au lieu d'envoyer à Kafka. Permet d'inspecter topic, clé et valeur.
    """

    def __init__(self):
        self.sent = []  # liste de tuples (topic, key, value)
        self.flushed = False
        self.closed = False

    def send(self, topic, value, key=None):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


# ------------------------------------------------------------
# Fixtures d'événements (données de test réutilisables)
# ------------------------------------------------------------

def _gps_event():
    return GpsEvent(
        vehicle_id="veh-123",
        driver_id="drv-456",
        lat=45.50,
        lon=-73.60,
        speed_kmh=30.0,
        timestamp="2026-01-01T00:00:00+00:00",
    )


def _delivery_event():
    return DeliveryEvent(
        package_id="pkg-789",
        order_id="ord-abc",
        vehicle_id="veh-123",
        status="picked_up",
        lat=45.50,
        lon=-73.60,
        timestamp="2026-01-01T00:00:00+00:00",
    )


def _order_event():
    return OrderEvent(
        order_id="ord-abc",
        package_id="pkg-789",
        dest_lat=45.55,
        dest_lon=-73.65,
        status="created",
        timestamp="2026-01-01T00:00:00+00:00",
    )


def _driver_event():
    return DriverEvent(
        driver_id="drv-456",
        vehicle_id="veh-123",
        status="online",
        lat=45.50,
        lon=-73.60,
        timestamp="2026-01-01T00:00:00+00:00",
    )


# ------------------------------------------------------------
# Tests de routage : bon topic + bonne clé
# ------------------------------------------------------------

def test_gps_route_vers_topic_gps_avec_cle_vehicle():
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.publish_gps(_gps_event())

    assert len(fake.sent) == 1
    topic, key, value = fake.sent[0]
    assert topic == config.kafka.topic_gps
    assert key == "veh-123"                    # clé = vehicle_id
    assert value["vehicle_id"] == "veh-123"


def test_delivery_route_vers_topic_delivery_avec_cle_package():
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.publish_delivery(_delivery_event())

    topic, key, value = fake.sent[0]
    assert topic == config.kafka.topic_delivery
    assert key == "pkg-789"                    # clé = package_id
    assert value["status"] == "picked_up"


def test_order_route_vers_topic_orders_avec_cle_package():
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.publish_order(_order_event())

    topic, key, value = fake.sent[0]
    assert topic == config.kafka.topic_orders
    assert key == "pkg-789"                    # clé = package_id
    assert value["order_id"] == "ord-abc"


def test_driver_route_vers_topic_drivers_avec_cle_driver():
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.publish_driver(_driver_event())

    topic, key, value = fake.sent[0]
    assert topic == config.kafka.topic_drivers
    assert key == "drv-456"                    # clé = driver_id
    assert value["status"] == "online"


# ------------------------------------------------------------
# Tests de sérialisation et de cycle de vie
# ------------------------------------------------------------

def test_valeur_envoyee_est_un_dict():
    """La valeur passée à send() doit être un dict (prêt pour JSON), pas l'objet."""
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.publish_gps(_gps_event())

    _, _, value = fake.sent[0]
    assert isinstance(value, dict)
    # Tous les champs attendus sont présents.
    assert set(value.keys()) == {
        "vehicle_id", "driver_id", "lat", "lon", "speed_kmh", "timestamp"
    }


def test_flush_delegue_au_producteur_sous_jacent():
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.flush()

    assert fake.flushed is True


def test_close_delegue_au_producteur_sous_jacent():
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.close()

    assert fake.closed is True


def test_plusieurs_envois_sont_tous_captures():
    """Plusieurs publications successives sont toutes enregistrées, dans l'ordre."""
    fake = FakeBaseProducer()
    ep = EventProducer(producer=fake)

    ep.publish_gps(_gps_event())
    ep.publish_order(_order_event())
    ep.publish_delivery(_delivery_event())

    assert len(fake.sent) == 3
    assert fake.sent[0][0] == config.kafka.topic_gps
    assert fake.sent[1][0] == config.kafka.topic_orders
    assert fake.sent[2][0] == config.kafka.topic_delivery