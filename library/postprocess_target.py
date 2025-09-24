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
    "1": ("Enzyme", "Oxidoreductase", "Oxidoreductase"),
    "2": ("Enzyme", "Transferase", "Kinase"),
    "3": ("Enzyme", "Hydrolase", "Protease"),
    "4": ("Enzyme", "Lyase", "Lyase"),
    "5": ("Enzyme", "Isomerase", "Isomerase"),
    "6": ("Enzyme", "Ligase", "Ligase"),
}

EC_MAJOR_FALLBACK: Tuple[str, str, Optional[str]] = (
    "Other Protein Target",
    "Other Protein Target",
    None,
)

EC_MULTIFUNCTIONAL_CLASS: Tuple[str, str, Optional[str]] = (
    "Enzyme",
    "Multifunctional",
    "Multifunctional",
)

EC_FALLBACK_CONFIDENCE = "0.5"

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

PROTEIN_CLASSIFICATION_PATTERN = re.compile(
    r"protein_classification\"\s*:\s*\"([^\"]+)\"",
    flags=re.IGNORECASE,
)

CLASS_LABEL_MAP: dict[str, str] = {
    "enzyme": "Enzyme",
    "oxidoreductase": "Oxidoreductase",
    "hydrolase": "Hydrolase",
    "transferase": "Transferase",
    "ligase": "Ligase",
    "lyase": "Lyase",
    "isomerase": "Isomerase",
    "kinase": "Kinase",
    "protease": "Protease",
    "multifunctional": "Multifunctional",
    "ion channel": "Ion Channel",
    "voltage-gated ion channel": "Voltage-gated ion channel",
    "voltage-gated": "Voltage-gated",
    "ligand-gated ion channel": "Ligand-gated ion channel",
    "ligand-gated": "Ligand-gated",
    "transporter": "Transporter",
    "atpase": "ATPase",
    "atp-binding cassette transporter": "ATP-binding cassette transporter",
    "slc superfamily of solute carrier": "SLC superfamily of solute carrier",
    "receptor": "Receptor",
    "g protein-coupled receptor": "G protein-coupled receptor",
    "transcription factor": "Transcription factor",
    "tf: other": "TF: Other",
    "zinc finger": "Zinc finger",
    "other protein target": "Other Protein Target",
}

IUPHAR_TYPE_OVERRIDES: dict[str, Tuple[str, str, Optional[str]]] = {
    "enzyme.multifunctional": ("Enzyme", "Multifunctional", None),
    "enzyme.oxidoreductase": ("Enzyme", "Oxidoreductase", None),
    "enzyme.hydrolase": ("Enzyme", "Hydrolase", None),
    "enzyme.transferase": ("Enzyme", "Transferase", None),
    "enzyme.isomerase": ("Enzyme", "Isomerase", None),
    "enzyme.ligase": ("Enzyme", "Ligase", None),
    "enzyme.lyase": ("Enzyme", "Lyase", None),
    "transporter.atpase": ("Transporter", "ATPase", None),
    "transporter.atp-binding cassette transporter": (
        "Transporter",
        "ATP-binding cassette transporter",
        None,
    ),
    "transporter.slc superfamily of solute carrier": (
        "Transporter",
        "SLC superfamily of solute carrier",
        None,
    ),
    "ion channel.voltage-gated ion channel": (
        "Ion Channel",
        "Voltage-gated ion channel",
        None,
    ),
    "ion channel.ligand-gated ion channel": (
        "Ion Channel",
        "Ligand-gated ion channel",
        None,
    ),
    "receptor.g protein-coupled receptor": (
        "Receptor",
        "G protein-coupled receptor",
        None,
    ),
    "other protein target.other protein target": (
        "Other Protein Target",
        "Other Protein Target",
        None,
    ),
}

IUPHAR_TYPE_CHAIN_OVERRIDES: dict[
    Tuple[str, str], Tuple[str, str, Optional[str]]
] = {
    ("receptor.nuclear hormone receptor", "zinc finger"): (
        "Transcription factor",
        "Zinc finger",
        None,
    ),
}

IUPHAR_PAIR_OVERRIDES: dict[
    Tuple[str, str], Tuple[str, str, Optional[str]]
] = {
    ("receptor", "nuclear hormone receptor"): (
        "Transcription factor",
        "TF: Other",
        None,
    ),
    ("ion channel", "voltage-gated ion channel"): (
        "Ion Channel",
        "Voltage-gated ion channel",
        None,
    ),
    ("ion channel", "ligand-gated ion channel"): (
        "Ion Channel",
        "Ligand-gated ion channel",
        None,
    ),
    ("transporter", "atp-binding cassette transporter"): (
        "Transporter",
        "ATP-binding cassette transporter",
        None,
    ),
    ("transporter", "slc superfamily of solute carrier"): (
        "Transporter",
        "SLC superfamily of solute carrier",
        None,
    ),
    ("transporter", "atpase"): (
        "Transporter",
        "ATPase",
        None,
    ),
    ("other protein target", "other protein target"): (
        "Other Protein Target",
        "Other Protein Target",
        None,
    ),
    ("receptor", "g protein-coupled receptor"): (
        "Receptor",
        "G protein-coupled receptor",
        None,
    ),
}


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

    meta_columns = (
        "protein_class_pred_rule_id",
        "protein_class_pred_evidence",
        "protein_class_pred_confidence",
    )

    for column in (*class_columns, *meta_columns):
        if column not in result.columns:
            result[column] = pd.NA

    def _select_prediction(row: pd.Series) -> Optional[Dict[str, Any]]:
        candidates = [
            _infer_from_iuphar(row),
            _infer_from_protein_classifications(row),
            _infer_from_ec_numbers(row),
        ]
        for candidate in candidates:
            if candidate is None:
                continue
            has_class = any(
                not _is_empty(candidate.get(column)) for column in class_columns
            )
            if has_class:
                return candidate
        return None

    def _update_row(row: pd.Series) -> pd.Series:
        prediction = _select_prediction(row)
        if prediction is None:
            return row

        updated = False

        for column in class_columns:
            value = prediction.get(column)
            if _is_empty(row.get(column)) and not _is_empty(value):
                row[column] = value
                updated = True

        metadata_needed = any(_is_empty(row.get(column)) for column in meta_columns)

        if updated or metadata_needed:
            for column in meta_columns:
                value = prediction.get(column)
                if _is_empty(row.get(column)) and not _is_empty(value):
                    row[column] = value

        return row

    return result.apply(_update_row, axis=1)


def _infer_from_iuphar(row: pd.Series) -> Optional[Dict[str, Any]]:
    prediction = _infer_from_iuphar_type(row)
    if prediction:
        return prediction

    prediction = _infer_from_iuphar_class(row)
    if prediction:
        return prediction

    return _infer_from_iuphar_path(row)


def _infer_from_iuphar_type(row: pd.Series) -> Optional[Dict[str, Any]]:
    tokens = _tokenize_class_string(row.get("iuphar_type"))
    if not tokens:
        return None

    l1 = tokens[0] if tokens else None
    l2 = tokens[1] if len(tokens) > 1 else row.get("iuphar_subclass")
    l3 = tokens[2] if len(tokens) > 2 else row.get("iuphar_chain")

    return _finalize_iuphar_prediction(
        l1,
        l2,
        l3,
        row,
        rule_id="IUPHAR_TYPE",
        evidence="iuphar_type",
        confidence="1.0",
    )


def _infer_from_iuphar_class(row: pd.Series) -> Optional[Dict[str, Any]]:
    l1 = row.get("iuphar_class")
    l2 = row.get("iuphar_subclass")
    l3 = row.get("iuphar_chain")

    if _is_empty(l1) and _is_empty(l2) and _is_empty(l3):
        return None

    return _finalize_iuphar_prediction(
        l1,
        l2,
        l3,
        row,
        rule_id="IUPHAR_CLASS",
        evidence="iuphar_class",
        confidence="0.9",
    )


def _infer_from_iuphar_path(row: pd.Series) -> Optional[Dict[str, Any]]:
    tokens = _tokenize_class_string(row.get("iuphar_full_name_path"))
    if not tokens:
        return None

    l1 = tokens[0] if tokens else None
    l2 = tokens[1] if len(tokens) > 1 else None
    l3 = tokens[2] if len(tokens) > 2 else None

    return _finalize_iuphar_prediction(
        l1,
        l2,
        l3,
        row,
        rule_id="IUPHAR_PATH",
        evidence="iuphar_full_name_path",
        confidence="0.8",
    )


def _infer_from_protein_classifications(row: pd.Series) -> Optional[Dict[str, Any]]:
    tokens = PROTEIN_CLASSIFICATION_PATTERN.findall(_to_text(row.get("protein_classifications")))
    cleaned = [token for token in (_format_label(token) for token in tokens) if token]

    if not cleaned:
        return None

    while len(cleaned) < 3:
        cleaned.append(None)

    l1, l2, l3 = cleaned[:3]

    return _build_prediction(
        l1,
        l2,
        l3,
        rule_id="PROTEIN_CLASSIFICATION",
        evidence="protein_classifications",
        confidence="0.7",
    )


def _infer_from_ec_numbers(row: pd.Series) -> Optional[Dict[str, Any]]:
    family_key = _normalize_key(row.get("iuphar_family_id"))
    if family_key:
        return None

    cellularity = _normalize_key(row.get("cellularity"))
    if cellularity and cellularity != "multicellular":
        return None

    ec_numbers = row.get("reaction_ec_numbers")
    raw_majors = _extract_ec_majors(ec_numbers)
    majors = [major for major in raw_majors if major and major != "3"]

    if not majors:
        return None

    if len(majors) > 1:
        l1, l2, l3 = EC_MULTIFUNCTIONAL_CLASS
        return _build_prediction(
            l1,
            l2,
            l3,
            rule_id="EC_MAJOR_MULTI",
            evidence="reaction_ec_numbers",
            confidence="0.6",
        )

    major = majors[0]
    mapping = EC_MAJOR_CLASS_MAP.get(major)
    if mapping is None:
        l1, l2, l3 = EC_MAJOR_FALLBACK
        return _build_prediction(
            l1,
            l2,
            l3,
            rule_id="EC_MAJOR_FALLBACK",
            evidence="reaction_ec_numbers",
            confidence=EC_FALLBACK_CONFIDENCE,
        )

    l1, l2, l3 = mapping
    return _build_prediction(
        l1,
        l2,
        l3,
        rule_id="EC_MAJOR",
        evidence="reaction_ec_numbers",
        confidence="0.6",
    )


def _finalize_iuphar_prediction(
    l1: Any,
    l2: Any,
    l3: Any,
    row: pd.Series,
    *,
    rule_id: str,
    evidence: str,
    confidence: str,
) -> Optional[Dict[str, Any]]:
    type_key = _normalize_key(row.get("iuphar_type"))
    class_key = _normalize_key(row.get("iuphar_class"))
    subclass_key = _normalize_key(row.get("iuphar_subclass"))
    chain_key = _normalize_key(row.get("iuphar_chain"))

    resolved = _normalize_prediction_labels(
        l1,
        l2,
        l3,
        type_key=type_key,
        class_key=class_key,
        subclass_key=subclass_key,
        chain_key=chain_key,
    )

    if resolved is None:
        return None

    norm_l1, norm_l2, norm_l3 = resolved

    if _is_empty(norm_l1) and _is_empty(norm_l2) and _is_empty(norm_l3):
        return None

    return _build_prediction(
        norm_l1,
        norm_l2,
        norm_l3,
        rule_id=rule_id,
        evidence=evidence,
        confidence=confidence,
    )


def _normalize_prediction_labels(
    l1: Any,
    l2: Any,
    l3: Any,
    *,
    type_key: str,
    class_key: str,
    subclass_key: str,
    chain_key: str,
) -> Optional[Tuple[Optional[str], Optional[str], Optional[str]]]:
    norm_l1 = _format_label(l1)
    norm_l2 = _format_label(l2)
    norm_l3 = _format_label(l3)

    if type_key and chain_key and (type_key, chain_key) in IUPHAR_TYPE_CHAIN_OVERRIDES:
        override = IUPHAR_TYPE_CHAIN_OVERRIDES[(type_key, chain_key)]
        norm_l1, norm_l2, norm_l3 = override
    elif type_key and type_key in IUPHAR_TYPE_OVERRIDES:
        override = IUPHAR_TYPE_OVERRIDES[type_key]
        norm_l1, norm_l2, norm_l3 = override
    else:
        pair = (
            (norm_l1 or "").lower(),
            (norm_l2 or "").lower(),
        )
        if pair in IUPHAR_PAIR_OVERRIDES:
            override = IUPHAR_PAIR_OVERRIDES[pair]
            norm_l1, norm_l2, norm_l3 = override
        else:
            class_pair = (class_key, subclass_key)
            formatted_pair = (
                _format_label(class_pair[0]) or "",
                _format_label(class_pair[1]) or "",
            )
            if formatted_pair in IUPHAR_PAIR_OVERRIDES:
                override = IUPHAR_PAIR_OVERRIDES[formatted_pair]
                norm_l1, norm_l2, norm_l3 = override

    if not norm_l3 and chain_key not in {"", "n/a", "na"}:
        norm_l3 = _format_label(chain_key)

    return (norm_l1, norm_l2, norm_l3)


def _build_prediction(
    l1: Optional[str],
    l2: Optional[str],
    l3: Optional[str],
    *,
    rule_id: str,
    evidence: str,
    confidence: str,
) -> Dict[str, Any]:
    return {
        "protein_class_pred_L1": l1,
        "protein_class_pred_L2": l2,
        "protein_class_pred_L3": l3,
        "protein_class_pred_rule_id": rule_id,
        "protein_class_pred_evidence": evidence,
        "protein_class_pred_confidence": confidence,
    }


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


def _format_label(value: Any) -> Optional[str]:
    text = _to_text(value)
    if not text:
        return None

    lowered = text.lower()
    if lowered in {"n/a", "na"}:
        return None

    if lowered in CLASS_LABEL_MAP:
        return CLASS_LABEL_MAP[lowered]

    if ":" in text:
        prefix, suffix = [segment.strip() for segment in text.split(":", 1)]
        if suffix:
            normalized_suffix = CLASS_LABEL_MAP.get(suffix.lower(), suffix)
            if prefix.lower() in CLASS_LABEL_MAP:
                return normalized_suffix

    return text


def _normalize_class_value(value: Any) -> Optional[str]:
    return _format_label(value)


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


def _normalize_key(value: Any) -> str:
    lowered = _to_text(value).lower()
    if lowered in {"n/a", "na"}:
        return ""
    return lowered


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
