"""
Orchestrateur du simulateur.
Assemble générateurs et producteurs pour simuler une flotte de livraison
et publier les événements sur Kafka, en boucle, jusqu'à interruption (Ctrl+C).

Boucle principale (un « tick » toutes les SIM_TICK_SECONDS) :
  1. Éventuellement créer une nouvelle commande (topic orders).
  2. Pour chaque chauffeur, faire évoluer son état et publier :
       - déplacement GPS s'il livre (topic gps_positions),
       - transitions de livraison du colis (topic delivery_events),
       - transitions de statut du chauffeur (topic driver_events).
  3. Attendre le prochain tick.

Chaque chauffeur se voit affecter au plus une commande à la fois.
Quand il l'a livrée (ou qu'elle a échoué), il redevient disponible
pour la commande suivante en attente.
"""
import logging
import random
import signal
import time
from typing import Optional

from dotenv import load_dotenv

# Charge le .env AVANT d'importer la config (qui lit les variables d'env).
load_dotenv()

from simulator.config import config
from simulator.models import Driver, Order, DriverStatus
from simulator.generators.gps_generator import next_position, has_arrived
from simulator.generators.order_generator import new_order, should_create_order
from simulator.generators.delivery_generator import advance_delivery, is_terminal
from simulator.generators.driver_generator import (
    go_online,
    start_delivering,
    finish_delivering,
    toggle_break,
    go_offline,
    should_take_break,
    should_end_shift,
)
from simulator.producers.event_producer import EventProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulator")


class Simulator:
    """
    État et logique de la flotte simulée.

    Maintient :
      - la liste des chauffeurs et leur entité Driver,
      - la commande en cours d'affectation à chaque chauffeur (driver_id -> Order),
      - la file des commandes en attente d'un chauffeur libre.
    """

    def __init__(self, producer: Optional[EventProducer] = None) -> None:
        self.producer = producer or EventProducer()
        self.drivers: list[Driver] = []
        # Commande actuellement affectée à un chauffeur (par driver_id).
        self.assignments: dict[str, Order] = {}
        # Commandes créées mais pas encore affectées à un chauffeur.
        self.pending_orders: list[Order] = []
        self._running = True

    # ---- Initialisation ----------------------------------------------
    def setup_fleet(self) -> None:
        """Crée la flotte de chauffeurs et les met tous en ligne."""
        sim = config.simulator
        for _ in range(sim.nb_drivers):
            # Position de départ aléatoire dans les bornes de la ville.
            lat = random.uniform(sim.lat_min, sim.lat_max)
            lon = random.uniform(sim.lon_min, sim.lon_max)
            driver = Driver.create(lat=lat, lon=lon)
            self.drivers.append(driver)

            # Le chauffeur prend son service : offline -> online.
            event = go_online(driver)
            if event:
                self.producer.publish_driver(event)

        logger.info("Flotte initialisée : %d chauffeurs en ligne.", len(self.drivers))

    # ---- Gestion des commandes ---------------------------------------
    def maybe_create_order(self) -> None:
        """Crée peut-être une nouvelle commande et la publie sur le topic orders."""
        if should_create_order(probability=0.3):
            order, event = new_order()
            self.pending_orders.append(order)
            self.producer.publish_order(event)
            logger.info("Nouvelle commande %s -> file d'attente (%d en attente).",
                        order.package_id, len(self.pending_orders))

    def assign_orders(self) -> None:
        """
        Affecte les commandes en attente aux chauffeurs disponibles.
        Un chauffeur 'online' sans commande prend la première commande en file.
        """
        for driver in self.drivers:
            if not self.pending_orders:
                break
            if driver.status == DriverStatus.ONLINE and driver.driver_id not in self.assignments:
                order = self.pending_orders.pop(0)
                self.assignments[driver.driver_id] = order

                # Le chauffeur passe en livraison : online -> delivering.
                event = start_delivering(driver)
                if event:
                    self.producer.publish_driver(event)
                logger.info("Commande %s affectée au chauffeur %s.",
                            order.package_id, driver.driver_id)

    # ---- Évolution d'un chauffeur ------------------------------------
    def step_driver(self, driver: Driver) -> None:
        """Fait évoluer un chauffeur d'un tick selon son statut courant."""

        # Cas 1 : le chauffeur livre une commande.
        if driver.status == DriverStatus.DELIVERING:
            order = self.assignments.get(driver.driver_id)
            if order is None:
                # Sécurité : incohérence, on remet le chauffeur disponible.
                finish_delivering(driver)
                return

            # Déplacement GPS vers la destination de la commande.
            gps_event = next_position(driver, order.dest_lat, order.dest_lon)
            self.producer.publish_gps(gps_event)

            # Si première étape après affectation, publier le pickup.
            # (created -> picked_up -> in_transit se déroule au fil des ticks)
            if not is_terminal(order):
                delivery_event = advance_delivery(
                    order, driver.vehicle_id, driver.lat, driver.lon
                )
                if delivery_event:
                    self.producer.publish_delivery(delivery_event)

            # Arrivé à destination OU livraison terminale : on libère le chauffeur.
            if has_arrived(driver, order.dest_lat, order.dest_lon) or is_terminal(order):
                # S'assurer que la commande atteint un état terminal.
                while not is_terminal(order):
                    ev = advance_delivery(order, driver.vehicle_id, driver.lat, driver.lon)
                    if ev:
                        self.producer.publish_delivery(ev)

                del self.assignments[driver.driver_id]
                event = finish_delivering(driver)  # delivering -> online
                if event:
                    self.producer.publish_driver(event)
                logger.info("Chauffeur %s a terminé la commande %s (%s).",
                            driver.driver_id, order.package_id, order.status.value)
            return

        # Cas 2 : chauffeur disponible -> peut prendre une pause ou finir son quart.
        if driver.status == DriverStatus.ONLINE:
            if should_end_shift(probability=0.02):
                event = go_offline(driver)  # online -> offline
                if event:
                    self.producer.publish_driver(event)
                logger.info("Chauffeur %s a terminé son quart.", driver.driver_id)
            elif should_take_break(probability=0.05):
                event = toggle_break(driver)  # online -> on_break
                if event:
                    self.producer.publish_driver(event)
            return

        # Cas 3 : chauffeur en pause -> peut reprendre le service.
        if driver.status == DriverStatus.ON_BREAK:
            if random.random() < 0.3:  # 30 % de chance de reprendre à chaque tick
                event = toggle_break(driver)  # on_break -> online
                if event:
                    self.producer.publish_driver(event)
            return

    # ---- Boucle principale -------------------------------------------
    def run(self) -> None:
        """Lance la boucle de simulation jusqu'à interruption."""
        self.setup_fleet()
        tick = config.simulator.tick_seconds
        logger.info("Démarrage de la simulation (tick = %.1f s). Ctrl+C pour arrêter.", tick)

        try:
            while self._running:
                self.maybe_create_order()
                self.assign_orders()
                for driver in self.drivers:
                    self.step_driver(driver)
                self.producer.flush()
                time.sleep(tick)
        finally:
            self.shutdown()

    def stop(self, *_args) -> None:
        """Handler d'arrêt propre (SIGINT / SIGTERM)."""
        logger.info("Arrêt demandé, fermeture en cours...")
        self._running = False

    def shutdown(self) -> None:
        """Vide et ferme le producteur Kafka."""
        self.producer.close()
        logger.info("Simulation arrêtée proprement.")


def main() -> None:
    sim = Simulator()
    # Arrêt propre sur Ctrl+C (SIGINT) et sur SIGTERM (docker stop).
    signal.signal(signal.SIGINT, sim.stop)
    signal.signal(signal.SIGTERM, sim.stop)
    sim.run()


if __name__ == "__main__":
    main()