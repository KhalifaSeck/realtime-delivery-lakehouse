"""
Runner de validation Great Expectations.

Charge un type d'événement du lake (Parquet) avec pandas, applique la
suite d'attentes correspondante, et retourne le résultat de validation.

En GX 1.x, le flux est :
    contexte -> data source pandas -> data asset -> batch definition
    -> batch (à partir du DataFrame) -> validate(suite)

On encapsule ce boilerplate pour que la validation d'un type tienne
en un appel : validate_gps(context, df).
"""
import pandas as pd

from quality.gps_expectations import build_gps_suite
from quality.event_expectations import (
    build_delivery_suite,
    build_order_suite,
    build_driver_suite,
)

def _get_pandas_batch(context, df: pd.DataFrame, asset_name: str):
    """
    Construit un "batch" GX à partir d'un DataFrame pandas.

    Crée (ou récupère) une data source pandas, un asset dataframe, une
    batch definition, puis le batch concret alimenté par `df`.
    """
    # Data source pandas (une seule pour tout le runner).
    ds_name = "pandas_lake"
    try:
        data_source = context.data_sources.add_pandas(name=ds_name)
    except Exception:
        # Déjà créée (appels multiples) : on la récupère.
        data_source = context.data_sources.get(ds_name)

    # Asset "dataframe" : représente un DataFrame en mémoire.
    try:
        asset = data_source.add_dataframe_asset(name=asset_name)
    except Exception:
        asset = data_source.get_asset(asset_name)

    # Batch definition "whole dataframe" : tout le DataFrame en un batch.
    batch_def = asset.add_batch_definition_whole_dataframe(
        name=f"{asset_name}_batchdef"
    )

    # Batch concret : on passe le DataFrame via batch_parameters.
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    return batch


def validate_gps(context, df: pd.DataFrame) -> dict:
    """
    Valide un DataFrame GPS contre la suite d'attentes GPS.

    Retourne un dict résumé :
      - success       : True si toutes les attentes passent ;
      - n_expectations : nombre d'attentes évaluées ;
      - n_success      : nombre d'attentes réussies ;
      - results        : liste (type, colonne, succès) par attente.
    """
    suite = build_gps_suite(context)
    batch = _get_pandas_batch(context, df, asset_name="gps")

    result = batch.validate(suite)
    return _summarize(result)


def _summarize(result) -> dict:
    """Transforme un résultat de validation GX en résumé exploitable."""
    results_list = []
    for r in result.results:
        cfg = r.expectation_config
        results_list.append({
            "type": cfg.type,
            "column": cfg.kwargs.get("column", "?"),
            "success": r.success,
        })

    n_total = len(results_list)
    n_success = sum(1 for r in results_list if r["success"])

    return {
        "success": result.success,
        "n_expectations": n_total,
        "n_success": n_success,
        "results": results_list,
    }

def validate_delivery(context, df: pd.DataFrame) -> dict:
    """Valide un DataFrame delivery contre sa suite."""
    suite = build_delivery_suite(context)
    batch = _get_pandas_batch(context, df, asset_name="delivery")
    return _summarize(batch.validate(suite))


def validate_order(context, df: pd.DataFrame) -> dict:
    """Valide un DataFrame order contre sa suite."""
    suite = build_order_suite(context)
    batch = _get_pandas_batch(context, df, asset_name="order")
    return _summarize(batch.validate(suite))


def validate_driver(context, df: pd.DataFrame) -> dict:
    """Valide un DataFrame driver contre sa suite."""
    suite = build_driver_suite(context)
    batch = _get_pandas_batch(context, df, asset_name="driver")
    return _summarize(batch.validate(suite))