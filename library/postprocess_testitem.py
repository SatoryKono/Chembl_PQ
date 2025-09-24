from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from .postprocess_document import _prepare_activity
from .transforms import to_text
from .utils import coerce_types, finalize_aggregate_columns, safe_merge

logger = logging.getLogger(__name__)


def _aggregate_testitem(activity: pd.DataFrame) -> pd.DataFrame:
    if activity.empty or "document_chembl_id" not in activity.columns:
        return pd.DataFrame(columns=["document_chembl_id", "document_testitem_total"])
    valid = activity[activity["document_chembl_id"].notna()].copy()
    grouped = (
        valid.groupby("document_chembl_id", dropna=False)
        .agg(
            document_testitem_total=(
                "molecule_chembl_id",
                lambda x: x.dropna().nunique(),
            )
        )
        .reset_index()
    )
    aggregated = finalize_aggregate_columns(grouped, ["document_testitem_total"])
    return aggregated


def _compute_unknown_chirality(series: pd.Series, reference: int) -> pd.Series:
    def _transform(value: object) -> bool:
        if pd.isna(value):
            return False
        return bool((int(value) - reference) != 0)

    return series.apply(_transform)


def run(inputs: Dict[str, pd.DataFrame], config: dict) -> pd.DataFrame:
    testitem_df = inputs["testitem"].copy()
    activity_df = _prepare_activity(inputs.get("activity", pd.DataFrame()))

    logger.info("Starting testitem post-processing", extra={"rows": len(testitem_df)})

    base_schema = {
        "molecule_chembl_id": "string",
        "pref_name": "string",
        "molecule_type": "string",
        "structure_type": "string",
        "molecule_structures.standard_inchi_key": "string",
        "unknown_chirality": "string",        
        "nstereo": "Int64",
        "document_chembl_id": "string",
    }
    typed = coerce_types(testitem_df, base_schema)

    if "document_chembl_id" not in typed.columns:
        typed["document_chembl_id"] = pd.Series(
            pd.NA, index=typed.index, dtype="string"
        )

    fillable_columns = [
        "all_names",
        "molecule_structures.standard_inchi_key",
        "standard_inchi_key",
    ]
    for column in fillable_columns:
        if column in typed.columns:
            typed[column] = typed[column].fillna("")

    chirality_reference = int(
        config.get("pipeline", {}).get("testitem", {}).get("chirality_reference", 1)
    )
    typed["unknown_chirality"] = _compute_unknown_chirality(
        typed.get("nstereo", pd.Series([], dtype="Int64")), chirality_reference
    )

    aggregates = _aggregate_testitem(activity_df)
    enriched = safe_merge(
        typed,
        aggregates,
        on=["document_chembl_id"],
        how="left",
    )

    default_total = pd.Series(0, index=enriched.index, dtype="Int64")
    enriched["document_testitem_total"] = (
        pd.to_numeric(
            enriched.get("document_testitem_total", default_total),
            errors="coerce",
        )
        .fillna(0)
        .astype("Int64")
    )

    invalid_rules = (
        config.get("pipeline", {}).get("testitem", {}).get("invalid_rules", {})
    )
    molecule_type_expected = invalid_rules.get(
        "molecule_type", "small molecule"
    ).lower()
    structure_type_expected = invalid_rules.get("structure_type", "mol").lower()

    def _compute_invalid(row: pd.Series) -> bool:
        molecule_type = str(row.get("molecule_type", "")).lower()
        structure_type = str(row.get("structure_type", "")).lower()
        inchi_key = to_text(
            row.get(
                "molecule_structures.standard_inchi_key",
                row.get("standard_inchi_key", ""),
            )
        )
        return not (
            molecule_type == molecule_type_expected
            and structure_type == structure_type_expected
            and inchi_key != ""
        )

    enriched["invalid_record"] = enriched.apply(_compute_invalid, axis=1)

    if "nstereo" in enriched.columns:
        enriched = enriched.drop(columns=["nstereo"])

    column_types = config.get("pipeline", {}).get("testitem", {}).get("type_map", {})
    typed_result = coerce_types(enriched, column_types)

    column_order = (
        config.get("pipeline", {}).get("testitem", {}).get("column_order", [])
    )
    if column_order:
        missing = [col for col in column_order if col not in typed_result.columns]
        if missing:
            raise ValueError(f"Testitem output is missing columns: {missing}")
        typed_result = typed_result.loc[:, column_order]

    return typed_result


__all__ = ["run"]
