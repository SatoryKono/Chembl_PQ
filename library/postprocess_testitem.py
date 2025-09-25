from __future__ import annotations
import logging
from typing import Dict

import pandas as pd

# Changelog: 2024-09-25 — расширена типизация и вывод колонок для согласования с M-скриптом.
#             2024-10-05 — добавлен справочник для подстановки all_names/nstereo.
#             2024-10-10 — синхронизированы агрегации, нормализация текстов и проверка invalid_record.
#             2024-10-19 — убраны документные агрегаты из итогового набора данных.
from .transforms import normalize_pipe, normalize_string, to_text
from .utils import (
    coerce_types,
    ensure_columns,
)

logger = logging.getLogger(__name__)


def _compute_unknown_chirality(series: pd.Series, reference: int) -> pd.Series:
    def _transform(value: object) -> bool:
        if pd.isna(value):
            return False
        return bool((int(value) - reference) != 0)

    return series.apply(_transform)


def _prepare_reference(reference_df: pd.DataFrame | None) -> pd.DataFrame:
    schema = {
        "molecule_chembl_id": "string",
        "all_names": "string",
        "nstereo": "Int64",
    }
    if reference_df is None or reference_df.empty:
        return pd.DataFrame(columns=list(schema.keys()))

    completed = ensure_columns(reference_df, list(schema.keys()), schema)
    typed = coerce_types(completed, schema)
    filtered = typed[typed["molecule_chembl_id"].notna()].copy()
    selected = filtered.loc[:, list(schema.keys())]
    deduped = selected.drop_duplicates(subset=["molecule_chembl_id"])
    return deduped.reset_index(drop=True)


def _apply_reference(
    testitem_df: pd.DataFrame, reference_df: pd.DataFrame
) -> pd.DataFrame:
    if reference_df.empty or "molecule_chembl_id" not in testitem_df.columns:
        return testitem_df

    result = testitem_df.copy()
    reference_index = reference_df.set_index("molecule_chembl_id")
    override_columns = [
        column
        for column in ("all_names", "nstereo")
        if column in reference_index.columns
    ]

    if not override_columns:
        return result

    molecule_ids = result.get("molecule_chembl_id")
    for column in override_columns:
        mapped = molecule_ids.map(reference_index[column])
        if column in result.columns:
            result[column] = mapped.where(mapped.notna(), result[column])
        else:
            result[column] = mapped

    type_spec = {
        "all_names": "string",
        "nstereo": "Int64",
    }
    return coerce_types(result, {col: type_spec[col] for col in override_columns})


def run(inputs: Dict[str, pd.DataFrame], config: dict) -> pd.DataFrame:
    testitem_df = inputs["testitem"].copy()
    reference_df = _prepare_reference(inputs.get("testitem_reference"))

    logger.info("Starting testitem post-processing", extra={"rows": len(testitem_df)})

    base_schema = {
        "molecule_chembl_id": "string",
        "pref_name": "string",
        "all_names": "string",
        "molecule_structures.canonical_smiles": "string",
        "molecule_type": "string",
        "structure_type": "string",
        "is_radical": "boolean",
        "molecule_structures.standard_inchi_key": "string",
        "standard_inchi_key": "string",
        "unknown_chirality": "string",
        "nstereo": "Int64",
        "document_chembl_id": "string",
    }
    typed = coerce_types(testitem_df, base_schema)
    typed = _apply_reference(typed, reference_df)

    cleaning_config = config.get("cleaning", {})
    sort_pipes = bool(cleaning_config.get("sort_pipes", True))
    pipeline_testitem = config.get("pipeline", {}).get("testitem", {})

    if "pref_name" in typed.columns:
        typed["pref_name"] = typed["pref_name"].map(normalize_string).astype("string")

    if "all_names" in typed.columns:
        typed["all_names"] = (
            typed["all_names"]
            .map(lambda value: normalize_pipe(value, sort=sort_pipes))
            .astype("string")
        )

    if "document_chembl_id" not in typed.columns:
        typed["document_chembl_id"] = pd.Series(
            pd.NA, index=typed.index, dtype="string"
        )
 
    canonical_candidates = [
        column
        for column in (
            "canonical_smiles",
            "molecule_structures.canonical_smiles",
        )
        if column in typed.columns
    ]

    if canonical_candidates:
        source_column = canonical_candidates[0]
        typed["canonical_smiles"] = typed[source_column].astype("string")
    else:
        typed["canonical_smiles"] = pd.Series(
            pd.NA, index=typed.index, dtype="string"
        )

    chirality_reference = int(
        config.get("pipeline", {}).get("testitem", {}).get("chirality_reference", 1)
    )
 
    chirality_reference = int(pipeline_testitem.get("chirality_reference", 1))
 
    typed["unknown_chirality"] = _compute_unknown_chirality(
        typed.get("nstereo", pd.Series([], dtype="Int64")), chirality_reference
    )

    invalid_rules = pipeline_testitem.get("invalid_rules", {})
    molecule_type_expected = invalid_rules.get("molecule_type")
    structure_type_expected = invalid_rules.get("structure_type")

    def _matches(value: object, expected: object) -> bool:
        if expected is None:
            return True
        if pd.isna(value):
            return False
        return value == expected

    def _compute_invalid(row: pd.Series) -> bool:
        molecule_type_value = row.get("molecule_type")
        structure_type_value = row.get("structure_type")

        inchi_candidate = row.get("molecule_structures.standard_inchi_key")
        if pd.isna(inchi_candidate) or inchi_candidate == "":
            inchi_candidate = row.get("standard_inchi_key")
        if pd.isna(inchi_candidate):
            inchi_key = ""
        else:
            inchi_key = to_text(inchi_candidate)

        return not (
            _matches(molecule_type_value, molecule_type_expected)
            and _matches(structure_type_value, structure_type_expected)
            and inchi_key != ""
        )

    typed["invalid_record"] = typed.apply(_compute_invalid, axis=1)

    skeleton_length = int(pipeline_testitem.get("skeleton_length", 14))

    def _compute_skeleton(value: object) -> object:
        if pd.isna(value):
            return pd.NA
        text = to_text(value)
        if text == "":
            return pd.NA
        return text[:skeleton_length]

    if "standard_inchi_key" in typed.columns:
        typed["skeleton_inchi_key"] = (
            typed["standard_inchi_key"].map(_compute_skeleton).astype("string")
        )
    else:
        typed["skeleton_inchi_key"] = pd.Series(
            pd.NA, index=typed.index, dtype="string"
        )

    processed = typed.drop(columns=["nstereo"], errors="ignore")

    columns_to_remove = [
        "document_chembl_id",
        "document_testitem_total",
        "molecule_structures.canonical_smiles",
    ]
    processed = processed.drop(
        columns=[col for col in columns_to_remove if col in processed.columns]
    )

    column_types = pipeline_testitem.get("type_map", {})
    column_order = pipeline_testitem.get("column_order", [])

    required_columns = column_order or list(column_types.keys())
    completed = ensure_columns(processed, required_columns, column_types)
    typed_result = coerce_types(completed, column_types)

    if column_order:
        missing = [col for col in column_order if col not in typed_result.columns]
        if missing:
            raise ValueError(f"Testitem output is missing columns: {missing}")
        typed_result = typed_result.loc[:, column_order]

    return typed_result


__all__ = ["run"]
