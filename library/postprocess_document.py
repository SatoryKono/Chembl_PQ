from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from .transforms import clean_pipe, to_text
from .utils import coerce_types, ensure_columns, finalize_aggregate_columns

logger = logging.getLogger(__name__)


ACTIVITY_SCHEMA = {
    "activity_chembl_id": "Int64",
    "assay_chembl_id": "string",
    "molecule_chembl_id": "string",
    "document_chembl_id": "string",
    "is_citation": "boolean",
}


def _prepare_activity(activity: pd.DataFrame) -> pd.DataFrame:
    if activity.empty:
        return pd.DataFrame(columns=ACTIVITY_SCHEMA.keys()).astype(ACTIVITY_SCHEMA)

    prepared = activity.copy()

    rename_map = {}
    if "activity_chembl_id" not in prepared.columns:
        for candidate in ("activity_id", "ACTIVITY_ID"):
            if candidate in prepared.columns:
                rename_map[candidate] = "activity_chembl_id"
                break
    if rename_map:
        prepared = prepared.rename(columns=rename_map)

    for column, dtype in ACTIVITY_SCHEMA.items():
        if column not in prepared.columns:
            if dtype == "boolean":
                prepared[column] = False
            else:
                prepared[column] = pd.NA

    typed = coerce_types(prepared, ACTIVITY_SCHEMA)
    return typed[list(ACTIVITY_SCHEMA.keys())]


def _aggregate_activity(
    activity: pd.DataFrame, thresholds: pd.DataFrame
) -> pd.DataFrame:
    if activity.empty:
        base = pd.DataFrame(
            columns=[
                "document_chembl_id",
                "n_activity",
                "citations",
                "n_assay",
                "n_testitem",
                "K_min_significant",
                "significant_citations_fraction",
            ]
        )
        return base.astype(
            {
                "document_chembl_id": "string",
                "n_activity": "Int64",
                "citations": "Int64",
                "n_assay": "Int64",
                "n_testitem": "Int64",
                "K_min_significant": "Int64",
                "significant_citations_fraction": "boolean",
            }
        )

    required_columns = [
        "document_chembl_id",
        "activity_chembl_id",
        "is_citation",
        "assay_chembl_id",
        "molecule_chembl_id",
    ]
    activity = ensure_columns(activity.copy(), required_columns, ACTIVITY_SCHEMA)
    activity = coerce_types(activity, ACTIVITY_SCHEMA)
    activity = activity[activity["document_chembl_id"].notna()]

    grouped = (
        activity.groupby("document_chembl_id", dropna=False)
        .agg(
            n_activity=("activity_chembl_id", "count"),
            citations=("is_citation", lambda x: x.fillna(False).astype(bool).sum()),
            n_assay=("assay_chembl_id", lambda x: x.dropna().nunique()),
            n_testitem=("molecule_chembl_id", lambda x: x.dropna().nunique()),
        )
        .reset_index()
    )

    thresholds_typed = coerce_types(
        thresholds, {"N": "Int64", "K_min_significant": "Int64"}
    )
    aggregated = grouped.merge(
        thresholds_typed.rename(columns={"N": "n_activity"}),
        on="n_activity",
        how="left",
    )
    aggregated["K_min_significant"] = (
        aggregated["K_min_significant"].fillna(0).astype("Int64")
    )
    aggregated["significant_citations_fraction"] = aggregated["citations"].fillna(
        0
    ).astype("Int64") > aggregated["K_min_significant"].fillna(0).astype("Int64")
    aggregated = finalize_aggregate_columns(
        aggregated,
        ["n_activity", "citations", "n_assay", "n_testitem"],
    )
    aggregated["significant_citations_fraction"] = aggregated[
        "significant_citations_fraction"
    ].astype("boolean")
    return aggregated


def _apply_classification_rules(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    rules = (
        config.get("pipeline", {}).get("document", {}).get("classification_rules", [])
    )
    if not rules:
        return df
    sort_pipes = config.get("cleaning", {}).get("sort_pipes", True)

    result = df.copy()
    for rule in rules:
        column = rule.get("column")
        if column not in result.columns:
            continue
        alias_map = rule.get("alias")
        drop_list = rule.get("drop")
        logger.debug("Normalizing pipe column", extra={"column": column})
        result[column] = clean_pipe(
            result[column],
            alias_map=alias_map,
            drop_list=drop_list,
            sort=bool(sort_pipes),
        )
    return result


def _compute_responses(
    row: pd.Series, response_columns: list[str], base_weight: int
) -> int:
    non_empty = 0
    for column in response_columns:
        value = to_text(row.get(column, ""))
        if value:
            non_empty += 1
    return base_weight + non_empty


def _compute_review(row: pd.Series, base_weight: int, threshold: float) -> bool:
    pub_type = to_text(row.get("PubMed.publication_type", ""))
    scholar_type = to_text(row.get("scholar.PublicationTypes", ""))
    openalex_type = to_text(row.get("OpenAlex.publication_type", ""))
    openalex_cross = to_text(row.get("OpenAlex.crossref_type", ""))
    base_review = bool(row.get("review", False))
    n_responses = row.get("n_responces", 1)

    def contains_review(value: str) -> bool:
        tokens = [token.strip() for token in value.split("|") if token.strip()]
        return "review" in tokens

    vote_score = (
        int(contains_review(pub_type))
        + int(scholar_type == "review")
        + int(openalex_type == "review")
        + int(openalex_cross == "review")
        + (base_weight * int(base_review))
    )
    normalized_score = vote_score / n_responses if n_responses else 0
    return bool(base_review or normalized_score > threshold)


def run(inputs: Dict[str, pd.DataFrame], config: dict) -> pd.DataFrame:
    document_df = inputs["document"].copy()
    activity_df = inputs.get("activity", pd.DataFrame())
    thresholds_df = inputs.get("citation_fraction", pd.DataFrame())

    logger.info("Starting document post-processing", extra={"rows": len(document_df)})

    activity_prepared = _prepare_activity(activity_df)
    aggregates = _aggregate_activity(activity_prepared, thresholds_df)

    merged = document_df.merge(
        aggregates,
        left_on="ChEMBL.document_chembl_id",
        right_on="document_chembl_id",
        how="left",
    )
    merged = merged.drop(columns=["document_chembl_id"], errors="ignore")

    merged = merged.fillna(
        {
            "n_activity": 0,
            "citations": 0,
            "n_assay": 0,
            "n_testitem": 0,
            "K_min_significant": 0,
            "significant_citations_fraction": False,
        }
    )

    normalized = _apply_classification_rules(merged, config)

    review_cfg = config.get("pipeline", {}).get("document", {}).get("review", {})
    base_weight = int(review_cfg.get("base_weight", 2))
    threshold = float(review_cfg.get("threshold", 0.335))
    response_columns = review_cfg.get("response_columns", [])

    if "review" not in normalized.columns:
        normalized["review"] = False
    normalized["review"] = normalized["review"].fillna(False).astype("boolean")
    normalized["n_responces"] = normalized.apply(
        lambda row: _compute_responses(row, response_columns, base_weight), axis=1
    )
    normalized["review"] = normalized.apply(
        lambda row: _compute_review(row, base_weight, threshold), axis=1
    )
    normalized["is_experimental"] = ~normalized["review"].astype(bool)

    if "K_min_significant" in normalized.columns:
        normalized = normalized.drop(columns=["K_min_significant"])

    document_cfg = config.get("pipeline", {}).get("document", {})
    column_types = document_cfg.get("type_map", {})

    required_columns: list[str] = []
    if column_types:
        required_columns.extend(column_types.keys())

    column_order = document_cfg.get("column_order", [])
    if column_order:
        required_columns.extend(column_order)

    if required_columns:
        # Deduplicate while preserving the first occurrence order.
        ordered_unique = list(dict.fromkeys(required_columns))
        normalized = ensure_columns(normalized, ordered_unique, column_types)

    typed = coerce_types(normalized, column_types)

    formatters = document_cfg.get("formatters", {})
    zero_pad = formatters.get("zero_pad", {})
    for column, width in zero_pad.items():
        if column in typed.columns:
            typed[column] = (
                typed[column]
                .astype("string")
                .fillna("")
                .map(
                    lambda value, width=int(width): value.zfill(width)
                    if value != ""
                    else value
                )
                .astype("string")
            )

    column_order = document_cfg.get("column_order", [])
    if column_order:
        typed = ensure_columns(typed, column_order, column_types)
        missing = [col for col in column_order if col not in typed.columns]
        if missing:
            raise ValueError(f"Document output is missing columns: {missing}")
        typed = typed.loc[:, column_order]

    return typed


__all__ = ["run"]
