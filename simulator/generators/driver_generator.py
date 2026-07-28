"""
Générateur d'événements de chauffeur.
Gère le cycle de vie d'un chauffeur au cours de son quart et produit
le DriverEvent correspondant à chaque changement de statut.

Cycle de vie typique :
    offline -> online -> delivering -> online -> on_break -> online -> ... -> offline

Contrairement au cycle de livraison (linéaire), un chauffeur oscille :
il enchaîne des livraisons, prend des pauses, puis termine son quart.
Ce générateur fournit les transitions plausibles ; la boucle principale
décidera quand les déclencher.
"""
import random

from simulator.models import (
    Driver,
    DriverEvent,
    DriverStatus,
    _now_iso,
)


def _make_event(driver: Driver) -> DriverEvent:
    """Fabrique un DriverEvent reflétant le statut courant du chauffeur."""
    return DriverEvent(
        driver_id=driver.driver_id,
        vehicle_id=driver.vehicle_id,
        status=driver.status.value,
        lat=round(driver.lat, 6),
        lon=round(driver.lon, 6),
        timestamp=_now_iso(),
    )


def go_online(driver: Driver) -> DriverEvent | None:
    """
    Met un chauffeur hors ligne en ligne (début de quart).
    Sans effet s'il est déjà en ligne : retourne None.
    """
    if driver.status != DriverStatus.OFFLINE:
        return None
    driver.status = DriverStatus.ONLINE
    return _make_event(driver)


def start_delivering(driver: Driver) -> DriverEvent | None:
    """
    Passe un chauffeur disponible (online) en cours de livraison.
    Valide seulement depuis ONLINE : sinon None.
    """
    if driver.status != DriverStatus.ONLINE:
        return None
    driver.status = DriverStatus.DELIVERING
    return _make_event(driver)


def finish_delivering(driver: Driver) -> DriverEvent | None:
    """
    Ramène un chauffeur en livraison vers l'état disponible (online),
    une fois le colis livré. Valide seulement depuis DELIVERING : sinon None.
    """
    if driver.status != DriverStatus.DELIVERING:
        return None
    driver.status = DriverStatus.ONLINE
    return _make_event(driver)


def toggle_break(driver: Driver) -> DriverEvent | None:
    """
    Bascule entre pause et disponibilité :
      - online   -> on_break (le chauffeur prend une pause)
      - on_break -> online   (il reprend le service)
    Sans effet dans les autres statuts : retourne None.
    """
    if driver.status == DriverStatus.ONLINE:
        driver.status = DriverStatus.ON_BREAK
    elif driver.status == DriverStatus.ON_BREAK:
        driver.status = DriverStatus.ONLINE
    else:
        return None
    return _make_event(driver)


def go_offline(driver: Driver) -> DriverEvent | None:
    """
    Termine le quart : ramène le chauffeur hors ligne.
    On n'autorise la fin de quart que depuis ONLINE ou ON_BREAK
    (pas en pleine livraison). Sinon None.
    """
    if driver.status not in (DriverStatus.ONLINE, DriverStatus.ON_BREAK):
        return None
    driver.status = DriverStatus.OFFLINE
    return _make_event(driver)


def should_take_break(probability: float = 0.05) -> bool:
    """Décide aléatoirement si un chauffeur disponible prend une pause à ce tick."""
    return random.random() < probability


def should_end_shift(probability: float = 0.02) -> bool:
    """Décide aléatoirement si un chauffeur termine son quart à ce tick."""
    return random.random() < probability