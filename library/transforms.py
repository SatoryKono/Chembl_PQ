from __future__ import annotations

import logging
import unicodedata
from typing import Any, Dict, Iterable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def to_text(value: Any) -> str:
    """Convert *value* to a trimmed lowercase string.

    The function mirrors the Power Query helper by treating ``None`` and ``NaN``
    values as empty strings. Non-string inputs are coerced via ``str``.
    """

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    normalized = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C")
    stripped = normalized.strip()
    return stripped


def normalize_whitespace(value: Any, lower: bool = True, strip: bool = True) -> str:
    """Clean control characters and optionally trim/lower text values."""

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""

    text = str(value)
    cleaned = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C")
    trimmed = cleaned.strip() if strip else cleaned
    return trimmed.lower() if lower else trimmed


def normalize_string(value: Any, lower: bool = True) -> Any:
    """Normalize scalar text values by trimming and optional lower-casing."""

    if value is None:
        return pd.NA
    if isinstance(value, float) and pd.isna(value):
        return pd.NA
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if lower:
        text = text.lower()
    if text == "":
        return pd.NA
    return text


def _normalize_token(value: Any, lower: bool = True) -> Optional[str]:
    text = to_text(value)
    if text == "":
        return None
    compact = " ".join(text.split())
    if lower:
        compact = compact.lower()
    return compact if compact else None


def _prepare_alias_map(
    alias_map: Optional[Dict[str, Optional[str]]],
) -> Dict[str, Optional[str]]:
    if not alias_map:
        return {}
    return {
        _normalize_token(key)
        or key: (_normalize_token(val) if isinstance(val, str) else val)
        for key, val in alias_map.items()
    }


def _prepare_drop_list(drop_list: Optional[Iterable[str]]) -> set[str]:
    drops: set[str] = set()
    if not drop_list:
        return drops
    for value in drop_list:
        normalized = _normalize_token(value)
        if normalized:
            drops.add(normalized)
    return drops


def clean_pipe(
    series: pd.Series,
    alias_map: Optional[Dict[str, Optional[str]]] = None,
    drop_list: Optional[Iterable[str]] = None,
    sort: bool = True,
) -> pd.Series:
    """Normalize pipe-delimited strings with aliasing and optional sorting."""

    alias_normalized = _prepare_alias_map(alias_map)
    drop_normalized = _prepare_drop_list(drop_list)

    def _process(value: Any) -> str:
        normalized = _normalize_token(value)
        if normalized is None:
            return ""
        parts = [token for token in normalized.split("|") if token != ""]
        cleaned: list[str] = []
        for part in parts:
            token = _normalize_token(part)
            if token is None:
                continue
            mapped = alias_normalized.get(token, token)
            if mapped is None:
                continue
            if mapped in drop_normalized:
                continue
            cleaned.append(mapped)
        if not cleaned:
            return ""
        if sort:
            cleaned = sorted(dict.fromkeys(cleaned))
        else:
            seen: set[str] = set()
            deduped: list[str] = []
            for token in cleaned:
                if token not in seen:
                    seen.add(token)
                    deduped.append(token)
            cleaned = deduped
        return "|".join(cleaned)

    return series.apply(_process)


def normalize_pipe(
    value: Any,
    alias_map: Optional[Dict[str, Optional[str]]] = None,
    drop_list: Optional[Iterable[str]] = None,
    sort: bool = True,
) -> Any:
    """Normalize pipe-delimited scalar values with aliasing and de-duplication."""

    alias_normalized = _prepare_alias_map(alias_map)
    drop_normalized = _prepare_drop_list(drop_list)

    if value is None:
        return pd.NA
    if isinstance(value, float) and pd.isna(value):
        return pd.NA
    if pd.isna(value):
        return pd.NA

    token = _normalize_token(value)
    if token is None:
        return pd.NA

    parts = [part.strip().lower() for part in token.split("|") if part.strip() != ""]
    if not parts:
        return pd.NA

    cleaned: list[str] = []
    for part in parts:
        mapped = alias_normalized.get(part, part)
        if mapped is None:
            continue
        if mapped in drop_normalized:
            continue
        cleaned.append(mapped)

    if not cleaned:
        return pd.NA

    if sort:
        cleaned = sorted(dict.fromkeys(cleaned))
    else:
        seen: set[str] = set()
        ordered: list[str] = []
        for item in cleaned:
            if item not in seen:
                seen.add(item)
                ordered.append(item)
        cleaned = ordered

    return "|".join(cleaned)


__all__ = [
    "to_text",
    "clean_pipe",
    "normalize_pipe",
    "normalize_string",
    "normalize_whitespace",
]
