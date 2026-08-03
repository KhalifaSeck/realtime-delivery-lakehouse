"""
Test manuel : valide les données GPS réelles du lake avec GX.
Usage : python -m quality._test_gps_validation
Prérequis : le lake contient des données (events/gps/).
Fichier temporaire (préfixe _), à supprimer après validation.
"""
import glob
import os

import pandas as pd
import great_expectations as gx

from streaming.config import LAKE_OUTPUT_DIR
from quality.runner import validate_gps


def main():
    # Charge tous les Parquet GPS du lake avec pandas.
    gps_path = os.path.join(LAKE_OUTPUT_DIR, "events", "gps")
    parquet_files = glob.glob(os.path.join(gps_path, "**", "*.parquet"), recursive=True)

    if not parquet_files:
        print(f"Aucun fichier Parquet GPS trouvé dans {gps_path}")
        print("Lance d'abord le pipeline (streaming.main + simulator) pour remplir le lake.")
        return

    print(f"Chargement de {len(parquet_files)} fichier(s) Parquet GPS...")
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    print(f"Total : {len(df)} lignes GPS chargées.")
    print(f"Colonnes : {list(df.columns)}")

    # Valide avec GX.
    context = gx.get_context(mode="ephemeral")
    summary = validate_gps(context, df)

    # Affiche le rapport.
    print("\n" + "=" * 50)
    print(f"VALIDATION GPS : {'✓ SUCCÈS' if summary['success'] else '✗ ÉCHEC'}")
    print(f"Attentes réussies : {summary['n_success']}/{summary['n_expectations']}")
    print("=" * 50)
    for r in summary["results"]:
        status = "✓" if r["success"] else "✗"
        print(f"  {status} {r['type']:45} [{r['column']}]")


if __name__ == "__main__":
    main()