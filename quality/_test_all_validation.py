"""
Test manuel : valide les 4 types du lake avec GX.
Usage : python -m quality._test_all_validation
Fichier temporaire, à supprimer après validation.
"""
import glob
import os

import pandas as pd
import great_expectations as gx

from streaming.config import LAKE_OUTPUT_DIR
from quality.runner import (
    validate_gps, validate_delivery, validate_order, validate_driver,
)


def _load(event_type: str) -> pd.DataFrame:
    path = os.path.join(LAKE_OUTPUT_DIR, "events", event_type)
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not files:
        return None
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def _report(name, summary):
    if summary is None:
        print(f"\n{name.upper()} : aucune donnée")
        return
    status = "✓ SUCCÈS" if summary["success"] else "✗ ÉCHEC"
    print(f"\n{name.upper()} : {status} ({summary['n_success']}/{summary['n_expectations']})")
    for r in summary["results"]:
        s = "✓" if r["success"] else "✗"
        print(f"  {s} {r['type']:42} [{r['column']}]")


def main():
    context = gx.get_context(mode="ephemeral")

    validators = {
        "gps": validate_gps,
        "delivery": validate_delivery,
        "order": validate_order,
        "driver": validate_driver,
    }

    for event_type, validate_fn in validators.items():
        df = _load(event_type)
        if df is None:
            print(f"\n{event_type.upper()} : aucune donnée dans le lake")
            continue
        summary = validate_fn(context, df)
        _report(event_type, summary)


if __name__ == "__main__":
    main()