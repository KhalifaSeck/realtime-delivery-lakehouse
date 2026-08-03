"""
Suites d'attentes GX pour les événements delivery, order et driver.

Chaque type a ses règles métier propres, cohérentes avec les statuts
définis dans simulator/models.py (enums DeliveryStatus, DriverStatus).

API : Great Expectations 1.x. Comme pour gps_expectations, chaque
fonction prend le contexte et y enregistre sa suite.
"""
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import expectations as gxe


# Statuts autorisés (cohérents avec les enums du simulateur).
_DELIVERY_STATUSES = ["created", "picked_up", "in_transit", "delivered", "failed"]
_ORDER_STATUSES = ["created"]
_DRIVER_STATUSES = ["offline", "online", "on_break", "delivering"]


def build_delivery_suite(context) -> ExpectationSuite:
    """
    Suite pour delivery_events :
      - package_id, order_id, vehicle_id : non nuls ;
      - status : dans l'ensemble autorisé.
    """
    suite = context.suites.add(ExpectationSuite(name="delivery_quality_suite"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="package_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="vehicle_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=_DELIVERY_STATUSES)
    )
    return suite


def build_order_suite(context) -> ExpectationSuite:
    """
    Suite pour orders :
      - order_id, package_id : non nuls ;
      - status : 'created' uniquement (une commande naît 'created').
    """
    suite = context.suites.add(ExpectationSuite(name="order_quality_suite"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="package_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=_ORDER_STATUSES)
    )
    return suite


def build_driver_suite(context) -> ExpectationSuite:
    """
    Suite pour driver_events :
      - driver_id, vehicle_id : non nuls ;
      - status : dans l'ensemble des statuts chauffeur autorisés.
    """
    suite = context.suites.add(ExpectationSuite(name="driver_quality_suite"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="driver_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="vehicle_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=_DRIVER_STATUSES)
    )
    return suite