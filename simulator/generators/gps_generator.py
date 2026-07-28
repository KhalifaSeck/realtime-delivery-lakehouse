import math
import random

from simulator.config import config
from simulator.models import GpsEvent, Driver, _now_iso


# Rayon de la Terre en kilomètres (pour les conversions distance <-> degrés)
_EARTH_RADIUS_KM = 6371.0

# Vitesse de croisière cible d'un véhicule de livraison urbain (km/h)
_CRUISE_SPEED_KMH = 30.0

# Tolérance d'arrivée : en deçà de cette distance, on considère la cible atteinte (km)
_ARRIVAL_THRESHOLD_KM = 0.05  # 50 mètres


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Distance en kilomètres entre deux points GPS (formule de Haversine).
    Utilisée pour mesurer la distance restante jusqu'à la destination.
    """
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return _EARTH_RADIUS_KM * c


def _step_towards(
    lat: float, lon: float, dest_lat: float, dest_lon: float, distance_km: float
) -> tuple[float, float]:
    """
    Calcule la nouvelle position après avoir parcouru `distance_km`
    en direction de la destination, en ligne droite.
    Retourne (nouvelle_lat, nouvelle_lon).
    """
    remaining = haversine_km(lat, lon, dest_lat, dest_lon)
    if remaining <= distance_km or remaining == 0:
        # On atteint (ou dépasse) la cible : on s'y cale exactement.
        return dest_lat, dest_lon

    # Fraction du trajet restant parcourue à ce pas.
    fraction = distance_km / remaining
    new_lat = lat + (dest_lat - lat) * fraction
    new_lon = lon + (dest_lon - lon) * fraction
    return new_lat, new_lon


def _add_jitter(lat: float, lon: float) -> tuple[float, float]:
    """
    Ajoute un léger bruit gaussien à la position (~quelques mètres)
    pour simuler l'imprécision GPS et éviter des lignes trop parfaites.
    """
    # 0.00003 degré ≈ 3 mètres environ
    jitter = 0.00003
    return (
        lat + random.gauss(0, jitter),
        lon + random.gauss(0, jitter),
    )


def next_position(driver: Driver, dest_lat: float, dest_lon: float) -> GpsEvent:
    """
    Produit la prochaine position GPS d'un véhicule en route vers sa destination.

    - Calcule la distance parcourue sur un tick à la vitesse de croisière.
    - Avance le véhicule vers la cible.
    - Applique un léger bruit GPS.
    - Modifie `driver.lat` / `driver.lon` en place (l'état avance).
    - Retourne l'événement GPS à publier sur Kafka.

    La vitesse renvoyée varie légèrement autour de la vitesse de croisière.
    """
    # Distance théorique parcourue pendant un tick :
    # vitesse (km/h) * durée du tick (h) = km.
    tick_hours = config.simulator.tick_seconds / 3600.0
    speed_kmh = max(0.0, random.gauss(_CRUISE_SPEED_KMH, 5.0))
    distance_km = speed_kmh * tick_hours

    new_lat, new_lon = _step_towards(
        driver.lat, driver.lon, dest_lat, dest_lon, distance_km
    )
    new_lat, new_lon = _add_jitter(new_lat, new_lon)

    # L'état du chauffeur avance.
    driver.lat = new_lat
    driver.lon = new_lon

    return GpsEvent(
        vehicle_id=driver.vehicle_id,
        driver_id=driver.driver_id,
        lat=round(new_lat, 6),
        lon=round(new_lon, 6),
        speed_kmh=round(speed_kmh, 1),
        timestamp=_now_iso(),
    )


def has_arrived(driver: Driver, dest_lat: float, dest_lon: float) -> bool:
    """Vrai si le véhicule est assez proche de la destination pour la considérer atteinte."""
    return haversine_km(driver.lat, driver.lon, dest_lat, dest_lon) <= _ARRIVAL_THRESHOLD_KM