from __future__ import annotations

import logging
import math
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .transforms import clean_pipe
from .utils import coerce_types, deduplicate, ensure_columns

logger = logging.getLogger(__name__)


EC_MAJOR_CLASS_MAP: dict[str, Tuple[str, str, Optional[str]]] = {
    "1": ("Enzyme", "Oxidoreductase", None),
    "2": ("Enzyme", "Transferase", None),
    "3": ("Enzyme", "Hydrolase", None),
    "4": ("Enzyme", "Lyase", None),
    "5": ("Enzyme", "Isomerase", None),
    "6": ("Enzyme", "Ligase", None),
}

UNICELLULAR_PHYLA: set[str] = {
    "ciliophora",
    "apicomplexa",
    "euglenozoa",
    "dinoflagellata",
    "alveolata",
    "bacillariophyta",
    "parabasalia",
    "metamonada",
    "choanoflagellata",
    "chlorophyta",
    "",
}

MULTICELLULAR_ANIMAL_PHYLA: set[str] = {
    "chordata",
    "arthropoda",
    "mollusca",
    "nematoda",
    "annelida",
    "echinodermata",
    "cnidaria",
    "platyhelminthes",
    "porifera",
    "ctenophora",
    "tardigrada",
    "onychophora",
    "bryozoa",
    "brachiopoda",
    "hemichordata",
    "rotifera",
    "nemertea",
    "sipuncula",
    "priapulida",
    "loricifera",
    "gastrotricha",
    "kinorhyncha",
    "spiralia",
    "ecdysozoa",
}

MULTICELLULAR_PLANT_PHYLA: set[str] = {
    "streptophyta",
    "tracheophyta",
    "bryophyta",
    "marchantiophyta",
}

MOSTLY_MULTICELLULAR_PHYLA: set[str] = {"rhodophyta"}

FUNGI_PHYLA: set[str] = {
    "ascomycota",
    "basidiomycota",
    "mucoromycota",
    "microsporidia",
    "chytridiomycota",
    "dikarya",
}

COMPONENT_DESCRIPTION_PATTERN = re.compile(
    r"\"component_description\"\s*:\s*\"([^\"]+)\"",
    flags=re.IGNORECASE,
)


def run(inputs: Dict[str, pd.DataFrame], config: dict) -> pd.DataFrame:
    target_df = inputs["target"].copy()

    logger.info("Starting target post-processing", extra={"rows": len(target_df)})

    target_df = _enrich_name_columns(target_df)
    target_df = _enrich_ec_annotations(target_df)
    target_df = _enrich_protein_class_predictions(target_df)

    if "synonyms" in target_df.columns:
        target_df["synonyms"] = clean_pipe(
            target_df["synonyms"],
            alias_map=None,
            drop_list=None,
            sort=config.get("cleaning", {}).get("sort_pipes", True),
        )

    output_columns = (
        config.get("pipeline", {}).get("target", {}).get("output_columns", [])
    )
    type_map = config.get("pipeline", {}).get("target", {}).get("type_map", {})

    if output_columns:
        target_df = ensure_columns(target_df, output_columns, type_map)

    typed = coerce_types(target_df, type_map)

    if output_columns:
        typed = typed.loc[:, output_columns]

    if "target_chembl_id" in typed.columns:
        typed = deduplicate(typed, ["target_chembl_id"])

    return typed


def _enrich_name_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    if result.empty:
        return result

    result["recommended_name"] = result.apply(
        lambda row: _first_non_empty(
            [
                row.get("recommended_name"),
                row.get("recommendedName"),
                row.get("pref_name"),
                row.get("protein_name_canonical"),
            ]
        ),
        axis=1,
    )

    result["gene_name"] = result.apply(
        lambda row: _normalize_lower(
            _first_non_empty(
                [
                    row.get("gene_name"),
                    row.get("geneName"),
                    _pipe_first_token(_strip_brackets(row.get("gene_symbol_list"))),
                ]
            )
        ),
        axis=1,
    )

    if "gene_symbol_list" in result.columns:
        result["gene_symbol_list"] = result["gene_symbol_list"].apply(
            lambda value: _strip_brackets(value).lower()
            if isinstance(value, str)
            else _strip_brackets(value)
        )

    synonym_columns = [
        "protein_name_canonical",
        "pref_name",
        "protein_name_alt",
        "gene_symbol_list",
    ]

    if "target_components" in result.columns:
        result["_component_synonyms"] = result["target_components"].apply(
            _extract_component_descriptions
        )
        synonym_columns.append("_component_synonyms")

    result["synonyms"] = result.apply(
        lambda row: _combine_synonyms(row, synonym_columns),
        axis=1,
    )

    if "_component_synonyms" in result.columns:
        result = result.drop(columns="_component_synonyms")

    return result


def _enrich_ec_annotations(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    if result.empty:
        return result

    if "reaction_ec_numbers" in result.columns:
        result["reaction_ec_numbers"] = result["reaction_ec_numbers"].apply(
            lambda value: _join_pipe_tokens(_split_pipe(value))
        )
    else:
        result["reaction_ec_numbers"] = ""

    result["cellularity"] = result.apply(
        lambda row: _classify_cellularity(
            row.get("lineage_superkingdom"),
            _candidate_lineage_values(
                [
                    row.get("lineage_phylum"),
                    row.get("lineage_class"),
                ]
            ),
        ),
        axis=1,
    )

    result["multifunctional_enzyme"] = result["reaction_ec_numbers"].apply(
        lambda value: "true" if len(_extract_ec_majors(value)) > 1 else "false"
    )

    return result


def _enrich_protein_class_predictions(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    if result.empty:
        return result

    class_columns = (
        "protein_class_pred_L1",
        "protein_class_pred_L2",
        "protein_class_pred_L3",
    )

    for column in class_columns:
        if column not in result.columns:
            result[column] = pd.NA

    for column in (
        "protein_class_pred_rule_id",
        "protein_class_pred_evidence",
        "protein_class_pred_confidence",
    ):
        if column not in result.columns:
            result[column] = pd.NA

    def _update_row(row: pd.Series) -> pd.Series:
        current_classes = [row.get(column) for column in class_columns]
        needs_update = any(_is_empty(value) for value in current_classes)

        if needs_update:
            inference = _infer_from_iuphar(row)
            if inference is None:
                inference = _infer_from_ec_numbers(row)
        else:
            inference = None

        if inference is None:
            return row

        (l1, l2, l3, rule_id, evidence, confidence) = inference
        updated = False

        for column, value in zip(class_columns, (l1, l2, l3)):
            if _is_empty(row.get(column)) and not _is_empty(value):
                row[column] = value
                updated = True

        if updated:
            if _is_empty(row.get("protein_class_pred_rule_id")) and rule_id:
                row["protein_class_pred_rule_id"] = rule_id
            if _is_empty(row.get("protein_class_pred_evidence")) and evidence:
                row["protein_class_pred_evidence"] = evidence
            if _is_empty(row.get("protein_class_pred_confidence")) and confidence:
                row["protein_class_pred_confidence"] = confidence

        return row

    return result.apply(_update_row, axis=1)


def _infer_from_iuphar(row: pd.Series) -> Optional[Tuple[str, str, Optional[str], str, str, str]]:
    tokens = _tokenize_class_string(row.get("iuphar_type"))
    evidence = "iuphar_type" if tokens else ""

    if not tokens:
        tokens = _tokenize_class_string(row.get("iuphar_class"))
        evidence = "iuphar_class" if tokens else evidence

    if not tokens:
        tokens = _tokenize_class_string(row.get("iuphar_full_name_path"))
        evidence = "iuphar_full_name_path" if tokens else evidence

    if not tokens:
        return None

    l1 = tokens[0] if tokens else None
    l2 = tokens[1] if len(tokens) > 1 else row.get("iuphar_subclass")
    l3 = tokens[2] if len(tokens) > 2 else row.get("iuphar_chain")

    return (
        _normalize_class_value(l1),
        _normalize_class_value(l2),
        _normalize_class_value(l3),
        "IUPHAR_TYPE" if evidence == "iuphar_type" else "IUPHAR_INFERRED",
        evidence,
        "1.0",
    )


def _infer_from_ec_numbers(row: pd.Series) -> Optional[Tuple[str, str, Optional[str], str, str, str]]:
    ec_numbers = row.get("reaction_ec_numbers")
    majors = _extract_ec_majors(ec_numbers)

    if not majors:
        return None

    if len(majors) > 1:
        return (
            "Enzyme",
            "Multifunctional",
            None,
            "EC_MAJOR_MULTI",
            "reaction_ec_numbers",
            "0.6",
        )

    major = majors[0]
    mapping = EC_MAJOR_CLASS_MAP.get(major)
    if not mapping:
        return None

    l1, l2, l3 = mapping
    return (
        l1,
        l2,
        l3,
        "EC_MAJOR",
        "reaction_ec_numbers",
        "0.6",
    )


def _combine_synonyms(row: pd.Series, columns: Sequence[str]) -> str:
    tokens: List[str] = []
    for column in columns:
        value = row.get(column)
        if column == "_component_synonyms" and isinstance(value, list):
            tokens.extend(_flatten_iterable(value))
        else:
            tokens.extend(_split_pipe(value))
    deduped = list(dict.fromkeys(token for token in tokens if token))
    return "|".join(deduped)


def _classify_cellularity(superkingdom: Any, candidates: Sequence[Any]) -> str:
    sk = _normalize_taxon_value(superkingdom)

    if sk == "":
        return "ambiguous"

    results = [
        _classify_by_lineage(sk, _normalize_taxon_value(candidate))
        for candidate in candidates
        if candidate is not None
    ]

    non_ambiguous = [value for value in results if value != "ambiguous"]
    if non_ambiguous:
        return non_ambiguous[0]

    if results:
        return results[0]

    return _classify_by_lineage(sk, "")


def _classify_by_lineage(superkingdom: str, phylum: str) -> str:
    if superkingdom == "viruses":
        return "acellular (virus)"
    if superkingdom in {"bacteria", "archaea"}:
        return "unicellular"
    if superkingdom == "eukaryota":
        if phylum in MULTICELLULAR_ANIMAL_PHYLA or phylum in MULTICELLULAR_PLANT_PHYLA:
            return "multicellular"
        if phylum in UNICELLULAR_PHYLA:
            return "unicellular"
        if phylum in FUNGI_PHYLA:
            return "multicellular"
        if phylum in MOSTLY_MULTICELLULAR_PHYLA:
            return "multicellular"
        return "ambiguous"
    if superkingdom == "":
        return "ambiguous"
    return "ambiguous"


def _candidate_lineage_values(values: Iterable[Any]) -> List[Any]:
    seen: set[Any] = set()
    ordered: List[Any] = []
    for value in values:
        if value in seen or _is_empty(value):
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _tokenize_class_string(value: Any) -> List[str]:
    text = _to_text(value)
    if not text:
        return []
    tokens = [token.strip() for token in re.split(r"[.:>]", text) if token.strip()]
    return tokens


def _extract_ec_majors(value: Any) -> List[str]:
    majors: List[str] = []
    for token in _split_pipe(value):
        if token:
            major = token.split(".")[0]
            if major:
                majors.append(major)
    return list(dict.fromkeys(majors))


def _extract_component_descriptions(value: Any) -> List[str]:
    text = _to_text(value)
    if not text:
        return []
    return COMPONENT_DESCRIPTION_PATTERN.findall(text)


def _split_pipe(value: Any) -> List[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if value is pd.NA:
        return []
    if isinstance(value, list):
        tokens: List[str] = []
        for item in value:
            tokens.extend(_split_pipe(item))
        return tokens
    text = str(value)
    stripped = text.strip()
    if stripped == "":
        return []
    if stripped.startswith("[") and stripped.endswith("]"):
        stripped = stripped[1:-1]
    parts = [segment.strip() for segment in stripped.split("|")]
    return [part for part in parts if part]


def _join_pipe_tokens(tokens: Sequence[str]) -> str:
    if not tokens:
        return ""
    deduped = list(dict.fromkeys(token for token in tokens if token))
    return "|".join(deduped)


def _strip_brackets(value: Any) -> str:
    text = _to_text(value)
    return text.replace("[", "").replace("]", "")


def _first_non_empty(values: Sequence[Any]) -> Optional[str]:
    for value in values:
        text = _to_text(value)
        if text:
            return text
    return None


def _pipe_first_token(value: Any) -> Optional[str]:
    tokens = _split_pipe(value)
    if not tokens:
        return None
    return tokens[0]


def _normalize_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return text.lower()


def _normalize_class_value(value: Any) -> Optional[str]:
    text = _to_text(value)
    return text if text else None


def _is_empty(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _normalize_taxon_value(value: Any) -> str:
    text = _to_text(value)
    return text.lower()


def _to_text(value: Any) -> str:
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _flatten_iterable(values: Iterable[Any]) -> List[str]:
    tokens: List[str] = []
    for value in values:
        if isinstance(value, (list, tuple)):
            tokens.extend(_flatten_iterable(value))
        else:
            token = _to_text(value)
            if token:
                tokens.append(token)
    return tokens


__all__ = ["run"]
