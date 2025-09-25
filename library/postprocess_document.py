from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from .transforms import clean_pipe, to_text
from .utils import coerce_types, ensure_columns, finalize_aggregate_columns

logger = logging.getLogger(__name__)


PMID_SOURCES: tuple[str, ...] = (
    "PMID",
    "PubMed.PMID",
    "ChEMBL.pubmed_id",
    "scholar.PMID",
    "crossref.PMID",
    "OpenAlex.PMID",
)


def _sanitize_digits(value: Any) -> str:
    text = to_text(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits


def _sanitize_pmid(value: Any) -> Optional[str]:
    digits = _sanitize_digits(value)
    return digits or None


def _normalize_doi(value: Any) -> Optional[str]:
    text = to_text(value)
    if not text:
        return None
    lowered = text.lower()
    for prefix in (
        "doi:",
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
    ):
        if lowered.startswith(prefix):
            lowered = lowered[len(prefix) :]
            break
    normalized = lowered.strip().replace(" ", "")
    if not normalized or "/" not in normalized:
        return None
    return normalized


def _merge_sources(document_out: pd.DataFrame) -> pd.DataFrame:
    if document_out.empty:
        return document_out.copy()

    prepared = document_out.copy()

    rename_map = {
        "PubMed.DOI": "PubMed.doi",
        "PubMed.ArticleTitle": "title",
        "PubMed.Abstract": "abstract",
        "PubMed.PublicationType": "publication_type",
        "PubMed.MeSH_Descriptors": "MeSH.descriptors",
        "PubMed.JournalISOAbbrev": "journal",
        "PubMed.Volume": "volume",
        "PubMed.Issue": "issue",
        "ChEMBL.authors": "authors",
        "ChEMBL.document_chembl_id": "ChEMBL.document_chembl_id",
        "ChEMBL.title": "ChEMBL.title",
        "ChEMBL.abstract": "ChEMBL.abstract",
        "ChEMBL.doi": "ChEMBL.doi",
        "ChEMBL.page": "ChEMBL.page",
        "ChEMBL.volume": "ChEMBL.volume",
        "ChEMBL.issue": "ChEMBL.issue",
        "scholar.DOI": "scholar.doi",
    }
    prepared = prepared.rename(columns=rename_map)

    if "PubMed.StartPage" in prepared.columns or "PubMed.EndPage" in prepared.columns:
        start = prepared.get("PubMed.StartPage", pd.Series(index=prepared.index))
        end = prepared.get("PubMed.EndPage", pd.Series(index=prepared.index))
        start_text = (
            start.apply(to_text) if not start.empty else pd.Series("", index=prepared.index)
        )
        end_text = (
            end.apply(to_text) if not end.empty else pd.Series("", index=prepared.index)
        )

        def _combine_pages(start_val: str, end_val: str) -> str:
            if start_val and end_val:
                return f"{start_val}-{end_val}"
            if start_val:
                return start_val
            if end_val:
                return end_val
            return ""

        prepared["page"] = [
            _combine_pages(s, e)
            for s, e in zip(start_text.tolist(), end_text.tolist())
        ]

    for column in ("PubMed.doi", "scholar.doi", "crossref.doi", "OpenAlex.doi", "ChEMBL.doi"):
        if column in prepared.columns:
            prepared[column] = prepared[column].apply(to_text)

    if "authors" in prepared.columns:
        prepared["authors"] = prepared["authors"].apply(to_text)

    pmid_values: list[Optional[str]] = []
    for _, row in prepared.iterrows():
        pmid: Optional[str] = None
        for candidate in PMID_SOURCES:
            if candidate not in prepared.columns:
                continue
            pmid = _sanitize_pmid(row.get(candidate))
            if pmid:
                break
        pmid_values.append(pmid)

    prepared["PMID"] = pmid_values
    prepared["PMID"] = prepared["PMID"].astype("object")

    return prepared


def _choose_text(candidates: Iterable[Any]) -> Optional[str]:
    for value in candidates:
        text = to_text(value)
        if text:
            return text
    return None


def _parse_int_candidate(value: Any) -> tuple[Optional[int], bool]:
    text = to_text(value)
    if not text:
        return (None, False)
    try:
        return (int(text), False)
    except ValueError:
        return (None, True)


def _validate_rows(document_df: pd.DataFrame) -> pd.DataFrame:
    if document_df.empty:
        return document_df.copy()

    priority = {
        "pm": 0,
        "crossref": 1,
        "openalex": 2,
        "chembl": 3,
        "scholar": 4,
    }
    source_names = {
        "pm": "PubMed",
        "crossref": "crossref",
        "openalex": "OpenAlex",
        "chembl": "ChEMBL",
        "scholar": "scholar",
    }

    records: list[dict[str, Any]] = []
    for _, row in document_df.iterrows():
        base = row.to_dict()

        doi_candidates = {
            "pm": row.get("PubMed.doi"),
            "chembl": row.get("ChEMBL.doi"),
            "scholar": row.get("scholar.doi"),
            "crossref": row.get("crossref.doi"),
            "openalex": row.get("OpenAlex.doi"),
        }
        doi_normalized = {key: _normalize_doi(value) for key, value in doi_candidates.items()}
        counts: dict[str, list[str]] = {}
        for key, value in doi_normalized.items():
            if value:
                counts.setdefault(value, []).append(key)

        selected_doi: Optional[str] = None
        selected_source: Optional[str] = None
        best_support = 0
        best_priority = len(priority)
        for doi_value, providers in counts.items():
            support = len(providers)
            provider_priority = min(priority[p] for p in providers)
            if support > best_support or (
                support == best_support and provider_priority < best_priority
            ):
                selected_doi = doi_value
                selected_source = source_names[
                    min(providers, key=lambda key: priority[key])
                ]
                best_support = support
                best_priority = provider_priority

        consensus_support = best_support
        consensus_doi = selected_doi
        invalid_doi = selected_doi is None

        title = _choose_text(
            [row.get("title"), row.get("ChEMBL.title"), row.get("crossref.title")]
        )
        abstract = _choose_text([row.get("abstract"), row.get("ChEMBL.abstract")])
        page = _choose_text([row.get("page"), row.get("ChEMBL.page")])

        new_volume: Optional[int] = None
        invalid_volume = False
        for candidate in (row.get("volume"), row.get("ChEMBL.volume")):
            parsed, invalid = _parse_int_candidate(candidate)
            if parsed is not None:
                new_volume = parsed
                invalid_volume = False
                break
            invalid_volume = invalid_volume or invalid

        new_issue: Optional[int] = None
        invalid_issue = False
        for candidate in (row.get("issue"), row.get("ChEMBL.issue")):
            parsed, invalid = _parse_int_candidate(candidate)
            if parsed is not None:
                new_issue = parsed
                invalid_issue = False
                break
            invalid_issue = invalid_issue or invalid

        records.append(
            {
                **base,
                "doi_same_count": best_support,
                "invalid_doi": invalid_doi,
                "reason": "" if not invalid_doi else "missing_doi",
                "selected_doi": selected_doi,
                "selected_source": selected_source,
                "consensus_doi": consensus_doi,
                "consensus_support": consensus_support,
                "pm_doi_norm": doi_normalized["pm"],
                "pm_valid": doi_normalized["pm"] is not None,
                "chembl_doi_norm": doi_normalized["chembl"],
                "chembl_valid": doi_normalized["chembl"] is not None,
                "scholar_doi_norm": doi_normalized["scholar"],
                "scholar_valid": doi_normalized["scholar"] is not None,
                "crossref_doi_norm": doi_normalized["crossref"],
                "crossref_valid": doi_normalized["crossref"] is not None,
                "openalex_doi_norm": doi_normalized["openalex"],
                "openalex_valid": doi_normalized["openalex"] is not None,
                "pm_doi_raw": to_text(doi_candidates["pm"]),
                "chembl_doi_raw": to_text(doi_candidates["chembl"]),
                "scholar_doi_raw": to_text(doi_candidates["scholar"]),
                "crossref_doi_raw": to_text(doi_candidates["crossref"]),
                "openalex_doi_raw": to_text(doi_candidates["openalex"]),
                "peers_valid_distinct": len(counts),
                "new_title": title,
                "new_abstract": abstract,
                "new_page": page,
                "new_volume": new_volume,
                "invalid_volume": invalid_volume,
                "new_issue": new_issue,
                "invalid_issue": invalid_issue,
                "PMID_for_validation": row.get("PMID"),
            }
        )

    return pd.DataFrame.from_records(records)


def _coalesce_text(target: pd.Series, fallback: pd.Series) -> pd.Series:
    target_text = target.fillna("").astype(str).str.strip()
    fallback_text = fallback.fillna("").astype(str).str.strip()
    result = target.copy()
    mask = target_text == ""
    result.loc[mask] = fallback_text.loc[mask]
    return result


def _build_completed(row: pd.Series) -> str:
    def component(field: str, digits: int) -> str:
        value = row.get(field)
        text = to_text(value)
        if not text:
            return "0" * digits
        digits_only = _sanitize_digits(text)
        normalized = digits_only or text
        return normalized.zfill(digits)

    completed_year = component("completed.year", 4)
    completed_month = component("completed.month", 2)
    completed_day = component("completed.day", 2)
    revised_year = component("revised.year", 4)
    revised_month = component("revised.month", 2)
    revised_day = component("revised.day", 2)
    chembl_year = component("ChEMBL.year", 4)

    has_completed = (
        completed_year != "0000"
        and completed_month != "00"
        and completed_day != "00"
    )
    has_revised = (
        revised_year != "0000"
        and revised_month != "00"
        and revised_day != "00"
    )

    completed_date = (
        f"{completed_year}-{completed_month}-{completed_day}" if has_completed else None
    )
    revised_date = (
        f"{revised_year}-{revised_month}-{revised_day}" if has_revised else None
    )
    chembl_date = f"{chembl_year}-00-00" if chembl_year != "0000" else None

    final_date = completed_date or revised_date or chembl_date
    return final_date or "0000-00-00"


def _build_sort_order(row: pd.Series) -> str:
    issn = to_text(row.get("ISSN")) or "unknown"
    completed_value = to_text(row.get("completed")) or "0000-00-00"
    pmid = _sanitize_digits(row.get("PMID"))
    pmid_value = pmid.zfill(8) if pmid else "00000000"
    return f"{issn}:{completed_value}:{pmid_value}"


def _build_validation_frame(validated: pd.DataFrame) -> pd.DataFrame:
    if validated.empty:
        return validated.copy()

    result = validated.copy()
    support = pd.to_numeric(result.get("consensus_support"), errors="coerce").fillna(0)
    invalid_issue = result.get("invalid_issue", False)
    invalid_volume = result.get("invalid_volume", False)
    result["invalid_record"] = (
        support <= 2
        | pd.Series(invalid_issue).fillna(False).astype(bool)
        | pd.Series(invalid_volume).fillna(False).astype(bool)
    )

    columns_to_remove = ["title", "abstract", "volume", "issue", "page"]
    existing_originals = [col for col in columns_to_remove if col in result.columns]
    if existing_originals:
        result = result.drop(columns=existing_originals)

    drop_noise = ["ChEMBL.doi", "scholar.doi", "OpenAlex.doi", "crossref.doi"]
    existing_noise = [col for col in drop_noise if col in result.columns]
    if existing_noise:
        result = result.drop(columns=existing_noise)

    rename_map = {
        "new_title": "_title",
        "new_abstract": "abstract_",
        "new_volume": "volume",
        "new_issue": "issue",
        "new_page": "page",
        "selected_doi": "doi",
    }
    existing_renames = {k: v for k, v in rename_map.items() if k in result.columns}
    result = result.rename(columns=existing_renames)

    if "PubMed.MeSH_Qualifiers" in result.columns and "OpenAlex.MeSH.qualifiers" in result.columns:
        result["PubMed.MeSH_Qualifiers"] = _coalesce_text(
            result["PubMed.MeSH_Qualifiers"], result["OpenAlex.MeSH.qualifiers"]
        )

    if {"completed.year", "completed.month", "completed.day"}.issubset(result.columns) or {
        "revised.year",
        "revised.month",
        "revised.day",
    }.issubset(result.columns):
        result["completed"] = result.apply(_build_completed, axis=1)
    else:
        result["completed"] = "0000-00-00"

    result["sort_order"] = result.apply(_build_sort_order, axis=1)

    verbose_columns = [
        "doi_same_count",
        "invalid_doi",
        "reason",
        "consensus_doi",
        "consensus_support",
        "pm_doi_norm",
        "pm_valid",
        "chembl_doi_norm",
        "chembl_valid",
        "scholar_doi_norm",
        "scholar_valid",
        "crossref_doi_norm",
        "crossref_valid",
        "openalex_doi_norm",
        "openalex_valid",
        "pm_doi_raw",
        "chembl_doi_raw",
        "scholar_doi_raw",
        "crossref_doi_raw",
        "openalex_doi_raw",
        "peers_valid_distinct",
    ]
    existing_verbose = [col for col in verbose_columns if col in result.columns]
    if existing_verbose:
        result = result.drop(columns=existing_verbose)

    ordered_columns = sorted(result.columns)
    result = result.loc[:, ordered_columns]
    return result


ACTIVITY_SCHEMA = {
    "activity_chembl_id": "Int64",
    "assay_chembl_id": "string",
    "molecule_chembl_id": "string",
    "document_chembl_id": "string",
    "is_citation": "boolean",
}


DOCUMENT_RENAME_MAP = {
    "_title": "title",
    "abstract_": "abstract",
    "MeSH.descriptors": "PubMed.MeSH",
    "OpenAlex.MeSH.descriptors": "OpenAlex.MeSH",
    "PubMed.MeSH_Qualifiers": "MeSH.qualifiers",
    "PubMed.ChemicalList": "chemical_list",
    "publication_type": "PubMed.publication_type",
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
            sort=False,
        )
    return result
def _compute_review(row: pd.Series, base_weight: int, threshold: float) -> bool:
    pub_type = to_text(row.get("PubMed.publication_type", ""))
    scholar_type = to_text(row.get("scholar.PublicationTypes", ""))
    openalex_type = to_text(row.get("OpenAlex.publication_type", ""))
    openalex_cross = to_text(row.get("OpenAlex.crossref_type", ""))
    base_review = bool(row.get("review", False))
    n_responses = row.get("n_responces", 1)

    def contains_review(value: str) -> bool:
        return "review" in value

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
    document_df = inputs.get("document", pd.DataFrame()).copy()
    if "document_out" in inputs:
        merged_sources = _merge_sources(inputs["document_out"])
        validated = _build_validation_frame(_validate_rows(merged_sources))
        rename_map = {
            "_title": "title",
            "abstract_": "abstract",
            "MeSH.descriptors": "PubMed.MeSH",
            "OpenAlex.MeSH.descriptors": "OpenAlex.MeSH",
            "PubMed.MeSH_Qualifiers": "MeSH.qualifiers",
            "PubMed.ChemicalList": "chemical_list",
            "publication_type": "PubMed.publication_type",
        }
        for source, target in rename_map.items():
            if source in validated.columns:
                validated = validated.rename(columns={source: target})
        if "PMID_for_validation" in validated.columns:
            validated = validated.drop(columns=["PMID_for_validation"])
        document_df = validated
    activity_df = inputs.get("activity", pd.DataFrame())
    thresholds_df = inputs.get("citation_fraction", pd.DataFrame())

    pipeline_cfg = config.get("pipeline", {})
    document_cfg = pipeline_cfg.get("document", {})

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

    review_cfg = document_cfg.get("review", {})
    base_weight = int(review_cfg.get("base_weight", 2))
    threshold = float(review_cfg.get("threshold", 0.335))
    response_columns = review_cfg.get("response_columns", [])

    if "review" not in normalized.columns:
        normalized["review"] = False
    normalized["review"] = normalized["review"].fillna(False).astype("boolean")
    if response_columns:
        response_frame = normalized.reindex(columns=response_columns, fill_value="")
        response_count = response_frame.apply(lambda column: column.map(to_text)).ne("").sum(
            axis=1
        )
    else:
        response_count = pd.Series(0, index=normalized.index)

    normalized["n_responces"] = (
        base_weight + response_count
    ).astype("Int64")
    normalized["review"] = normalized.apply(
        lambda row: _compute_review(row, base_weight, threshold), axis=1
    )
    normalized["is_experimental"] = ~normalized["review"].astype(bool)

    if "K_min_significant" in normalized.columns:
        normalized = normalized.drop(columns=["K_min_significant"])


    rename_map = document_cfg.get("rename_map", DOCUMENT_RENAME_MAP)
    if rename_map:
        normalized = normalized.rename(columns=rename_map)

    column_types = document_cfg.get("type_map", {})
    column_order = document_cfg.get("column_order", [])
    required_columns: set[str] = set(column_types.keys()) | set(column_order)
    if required_columns:
        normalized = ensure_columns(normalized, required_columns, column_types)


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


    if column_order:
        typed = ensure_columns(typed, column_order, column_types)
        missing = [col for col in column_order if col not in typed.columns]
        if missing:
            raise ValueError(f"Document output is missing columns: {missing}")
        typed = typed.loc[:, column_order]

    return typed


__all__ = ["run"]
