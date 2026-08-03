"""
Suite d'attentes (expectations) Great Expectations pour les événements GPS.

Définit les règles de qualité que les positions GPS du lake doivent
respecter, en cohérence avec la validation/nettoyage de la Brique 2.

API : Great Expectations 1.x. En 1.x, une ExpectationSuite doit être
enregistrée dans un Data Context. On fournit donc une fonction qui prend
le contexte en paramètre et y ajoute la suite GPS.
"""
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import expectations as gxe


# Bornes géographiques plausibles (cohérentes avec cleaning.py).
_LAT_MIN, _LAT_MAX = 45.0, 46.5
_LON_MIN, _LON_MAX = -74.5, -73.0


def build_gps_suite(context) -> ExpectationSuite:
    """
    Crée et enregistre la suite d'attentes GPS dans le contexte fourni.

    `context` : un Data Context GX actif (ex. gx.get_context()).

    Règles :
      - vehicle_id, driver_id : jamais nuls.
      - lat, lon : dans les bornes plausibles.
      - speed_kmh : positive ou nulle.

    Retourne l'ExpectationSuite enregistrée.
    """
    # Crée la suite DANS le contexte (obligatoire en GX 1.x).
    suite = context.suites.add(
        ExpectationSuite(name="gps_quality_suite")
    )

    # Ajoute les expectations à la suite enregistrée.
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="vehicle_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="driver_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="lat", min_value=_LAT_MIN, max_value=_LAT_MAX
        )
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="lon", min_value=_LON_MIN, max_value=_LON_MAX
        )
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="speed_kmh", min_value=0.0, max_value=None
        )
    )

    return suite