from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from .document import _prepare_activity
from ..validators import coerce_types, finalize_aggregate_columns

logger = logging.getLogger(__name__)


def _aggregate_assay(activity: pd.DataFrame) -> pd.DataFrame:
    if activity.empty or "assay_chembl_id" not in activity.columns:
        return pd.DataFrame(columns=["document_chembl_id", "document_assay_total"])
    valid = activity[activity["document_chembl_id"].notna()].copy()
    grouped = (
        valid.groupby("document_chembl_id", dropna=False)
        .agg(document_assay_total=("assay_chembl_id", lambda x: x.dropna().nunique()))
        .reset_index()
    )
    aggregated = finalize_aggregate_columns(grouped, ["document_assay_total"])
    return aggregated


def normalize_assay(inputs: Dict[str, pd.DataFrame], config: dict) -> pd.DataFrame:
    assay_df = inputs["assay"].copy()
    activity_df = _prepare_activity(inputs.get("activity", pd.DataFrame()))

    logger.info("Starting assay post-processing", extra={"rows": len(assay_df)})

    aggregates = _aggregate_assay(activity_df)
    enriched = assay_df.merge(
        aggregates,
        on="document_chembl_id",
        how="left",
    )
    enriched["document_assay_total"] = (
        enriched["document_assay_total"].fillna(0).astype("Int64")
    )

    drop_columns = config.get("pipeline", {}).get("assay", {}).get("drop_columns", [])
    if drop_columns:
        enriched = enriched.drop(
            columns=[col for col in drop_columns if col in enriched.columns]
        )

    column_types = config.get("pipeline", {}).get("assay", {}).get("type_map", {})
    typed = coerce_types(enriched, column_types)

    column_order = config.get("pipeline", {}).get("assay", {}).get("column_order", [])
    if column_order:
        missing = [col for col in column_order if col not in typed.columns]
        if missing:
            raise ValueError(f"Assay output is missing columns: {missing}")
        typed = typed.loc[:, column_order]

    return typed


__all__ = ["normalize_assay"]
