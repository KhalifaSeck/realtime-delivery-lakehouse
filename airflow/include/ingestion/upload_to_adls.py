"""
Upload du lake Parquet local vers ADLS Gen2.

Parcourt récursivement C:\\delivery-lake\\events\\ et copie chaque
fichier Parquet vers le container 'raw-events' d'ADLS, en préservant
l'arborescence (event_type / event_date / fichier.parquet).

Usage :
    python -m ingestion.upload_to_adls              # upload tout le lake
    python -m ingestion.upload_to_adls --dry-run    # simulation, aucun upload
    python -m ingestion.upload_to_adls --type gps   # uniquement le type GPS

Auth : SAS token depuis .env (variable ADLS_SAS_TOKEN).
"""
import argparse
import os
from pathlib import Path

from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# Charge les variables depuis .env (à la racine du projet).
load_dotenv()


# ============================================================
# Configuration lue depuis l'environnement
# ============================================================
_ADLS_ACCOUNT = os.getenv("ADLS_ACCOUNT_NAME")
_ADLS_CONTAINER = os.getenv("ADLS_CONTAINER")
_ADLS_SAS = os.getenv("ADLS_SAS_TOKEN")
_LAKE_LOCAL = Path(os.getenv("LAKE_LOCAL_PATH", r"C:\delivery-lake\events"))
_EVENT_TYPES = ["gps", "delivery", "order", "driver"]


def _get_datalake_client() -> DataLakeServiceClient:
    """
    Construit un client ADLS Gen2 authentifié via SAS token.

    URL de service :
        https://<account>.dfs.core.windows.net + SAS
    """
    account_url = f"https://{_ADLS_ACCOUNT}.dfs.core.windows.net"
    # Le SAS token peut commencer par '?' ou non ; le SDK gère les deux.
    return DataLakeServiceClient(account_url=account_url, credential=_ADLS_SAS)


def _iter_local_parquet(event_type: str | None):
    """
    Parcourt les fichiers Parquet du lake local.

    event_type=None : tous les types ; sinon un seul (gps/delivery/order/driver).
    Retourne des tuples (chemin_absolu, chemin_relatif_a_events).
    """
    types = [event_type] if event_type else _EVENT_TYPES
    for et in types:
        base = _LAKE_LOCAL / et
        if not base.exists():
            continue
        for parquet in base.rglob("*.parquet"):
            rel = parquet.relative_to(_LAKE_LOCAL)
            yield parquet, rel


def upload_lake(event_type: str | None = None, dry_run: bool = False) -> dict:
    """
    Upload récursif du lake local vers ADLS.

    Retourne un résumé : nombre de fichiers traités, taille totale, erreurs.
    """
    client = _get_datalake_client()
    file_system = client.get_file_system_client(_ADLS_CONTAINER)

    n_files = 0
    n_bytes = 0
    errors = []

    for local_path, rel_path in _iter_local_parquet(event_type):
        # Chemin ADLS : on utilise '/' comme séparateur (POSIX, pas Windows).
        remote_path = str(rel_path).replace("\\", "/")
        size = local_path.stat().st_size

        if dry_run:
            print(f"[DRY-RUN] {remote_path}  ({size:,} bytes)")
            n_files += 1
            n_bytes += size
            continue

        try:
            file_client = file_system.get_file_client(remote_path)
            with open(local_path, "rb") as f:
                file_client.upload_data(f, overwrite=True)
            n_files += 1
            n_bytes += size
            if n_files % 50 == 0:
                print(f"  ... {n_files} fichiers uploadés")
        except Exception as e:
            errors.append((remote_path, str(e)))
            print(f"[ERR] {remote_path}: {e}")

    return {
        "n_files": n_files,
        "n_bytes": n_bytes,
        "n_errors": len(errors),
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description="Upload du lake local vers ADLS.")
    parser.add_argument("--type", choices=_EVENT_TYPES, help="Ne uploader qu'un type.")
    parser.add_argument("--dry-run", action="store_true", help="Simulation, aucun upload.")
    args = parser.parse_args()

    # Vérifications préalables.
    if not _ADLS_ACCOUNT or not _ADLS_SAS:
        print("ERREUR : .env incomplet (ADLS_ACCOUNT_NAME ou ADLS_SAS_TOKEN manquant).")
        return 1
    if not _LAKE_LOCAL.exists():
        print(f"ERREUR : lake local introuvable ({_LAKE_LOCAL}).")
        return 1

    print(f"Source : {_LAKE_LOCAL}")
    print(f"Cible  : {_ADLS_ACCOUNT}/{_ADLS_CONTAINER}")
    print(f"Type(s): {args.type or 'tous'}")
    print(f"Mode   : {'DRY-RUN' if args.dry_run else 'UPLOAD'}\n")

    summary = upload_lake(event_type=args.type, dry_run=args.dry_run)

    print("\n" + "=" * 50)
    print(f"Fichiers  : {summary['n_files']}")
    print(f"Taille    : {summary['n_bytes'] / (1024*1024):.2f} MB")
    print(f"Erreurs   : {summary['n_errors']}")
    print("=" * 50)
    return 0 if summary["n_errors"] == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())