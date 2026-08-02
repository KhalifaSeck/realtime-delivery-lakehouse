"""
Client Kafka de base pour le simulateur.
Encapsule la connexion, la sérialisation JSON et l'envoi de messages
avec clé de partition. Les producteurs métier (event_producer) s'appuient
dessus pour router les événements vers les bons topics.

Dépendance : kafka-python (ajoutée aux requirements en fin de brique).
"""
import json
import logging
from typing import Any, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.config import config

logger = logging.getLogger(__name__)


class BaseProducer:
    """
    Enveloppe légère autour de KafkaProducer.

    - Sérialise les valeurs en JSON UTF-8.
    - Encode la clé de partition en bytes (UTF-8) si fournie.
    - Journalise les échecs d'envoi sans faire planter le simulateur.
    """

    def __init__(self, bootstrap_servers: Optional[str] = None) -> None:
        """
        Initialise le producteur Kafka.
        `bootstrap_servers` : surchargeable pour les tests ; par défaut,
        valeur de la config (donc du .env / localhost:9092).
        """
        servers = bootstrap_servers or config.kafka.bootstrap_servers
        self._producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            acks="all",             # attend l'accusé de toutes les répliques (durabilité)
            retries=3,              # retransmet en cas d'échec transitoire
            linger_ms=50,           # petite fenêtre de regroupement (throughput)
            max_block_ms=10000,     # échoue en 10s si buffer plein (évite gel de 60s)
        )
        logger.info("KafkaProducer connecté à %s", servers)

    def send(self, topic: str, value: dict[str, Any], key: Optional[str] = None) -> None:
        """
        Envoie un message sur un topic, avec clé de partition optionnelle.

        L'envoi est asynchrone (bufferisé par kafka-python). En cas d'erreur,
        on journalise sans interrompre le simulateur : perdre un point GPS
        n'est pas critique, garder le simulateur vivant l'est davantage.
        """
        try:
            future = self._producer.send(topic, key=key, value=value)
            # Callback d'erreur : journalise si l'envoi échoue côté broker.
            future.add_errback(self._on_send_error, topic=topic, key=key)
        except KafkaError as exc:
            logger.error("Échec d'envoi immédiat sur %s (clé=%s) : %s", topic, key, exc)

    def _on_send_error(self, exc: Exception, topic: str, key: Optional[str]) -> None:
        """Callback appelé si le broker rejette le message."""
        logger.error("Erreur broker sur %s (clé=%s) : %s", topic, key, exc)

    def flush(self) -> None:
        """Force l'envoi de tous les messages bufferisés. À appeler avant l'arrêt."""
        self._producer.flush()
        logger.debug("Producteur vidé (flush).")

    def close(self) -> None:
        """Vide le buffer puis ferme proprement la connexion Kafka."""
        self._producer.flush()
        self._producer.close()
        logger.info("KafkaProducer fermé.")