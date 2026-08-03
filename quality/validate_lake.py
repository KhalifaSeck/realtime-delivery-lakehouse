"""
Point d'entrée de la validation qualité du lake.

Valide les 4 types d'événements du lake Parquet contre leurs suites
d'attentes Great Expectations, puis produit :
  - un rapport lisible en console ;
  - un dict consolidé (exploitable pour un futur export Prometheus) ;
  - un Data Quality Score global (% d'attentes réussies, tous types
    confondus).

Lancement : python -m quality.validate_lake
Prérequis : le lake contient des données (events/<type>/).
"""
import glob
import os

import pandas as pd
import great_expectations as gx

from streaming.config import LAKE_OUTPUT_DIR
from quality.runner import (
    validate_gps, validate_delivery, validate_order, validate_driver,
)


# Association type d'événement -> fonction de validation.
_VALIDATORS = {
    "gps": validate_gps,
    "delivery": validate_delivery,
    "order": validate_order,
    "driver": validate_driver,
}


def _load_event_type(event_type: str) -> pd.DataFrame | None:
    """Charge tous les Parquet d'un type depuis le lake, ou None si absent."""
    path = os.path.join(LAKE_OUTPUT_DIR, "events", event_type)
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not files:
        return None
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def validate_all() -> dict:
    """
    Valide tous les types disponibles dans le lake.

    Retourne un rapport consolidé :
      - by_type            : résumé par type d'événement ;
      - total_expectations : nombre total d'attentes évaluées ;
      - total_success      : nombre total d'attentes réussies ;
      - quality_score      : % global (0-100) d'attentes réussies ;
      - all_passed         : True si aucun échec sur aucun type.
    """
    context = gx.get_context(mode="ephemeral")

    by_type = {}
    total_exp = 0
    total_ok = 0

    for event_type, validate_fn in _VALIDATORS.items():
        df = _load_event_type(event_type)
        if df is None:
            by_type[event_type] = {"status": "no_data"}
            continue

        summary = validate_fn(context, df)
        summary["n_rows"] = len(df)
        by_type[event_type] = summary
        total_exp += summary["n_expectations"]
        total_ok += summary["n_success"]

    quality_score = round(100.0 * total_ok / total_exp, 1) if total_exp else 0.0

    return {
        "by_type": by_type,
        "total_expectations": total_exp,
        "total_success": total_ok,
        "quality_score": quality_score,
        "all_passed": total_ok == total_exp and total_exp > 0,
    }


def print_report(report: dict) -> None:
    """Affiche le rapport consolidé de façon lisible."""
    print("\n" + "=" * 56)
    print("  RAPPORT DE QUALITÉ DES DONNÉES — DELIVERY LAKEHOUSE")
    print("=" * 56)

    for event_type, summary in report["by_type"].items():
        if summary.get("status") == "no_data":
            print(f"\n  {event_type.upper():10} : (aucune donnée)")
            continue
        mark = "✓" if summary["success"] else "✗"
        print(f"\n  {event_type.upper():10} {mark}  "
              f"{summary['n_success']}/{summary['n_expectations']} attentes  "
              f"({summary['n_rows']} lignes)")
        for r in summary["results"]:
            s = "✓" if r["success"] else "✗"
            print(f"      {s} {r['type']:42} [{r['column']}]")

    print("\n" + "-" * 56)
    score = report["quality_score"]
    global_mark = "✓" if report["all_passed"] else "✗"
    print(f"  {global_mark}  DATA QUALITY SCORE : {score}%  "
          f"({report['total_success']}/{report['total_expectations']} attentes)")
    print("=" * 56 + "\n")


def main():
    report = validate_all()
    print_report(report)
    # Code de sortie : 0 si tout passe, 1 sinon (utile pour CI/CD).
    import sys
    sys.exit(0 if report["all_passed"] else 1)


if __name__ == "__main__":
    main()