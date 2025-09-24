from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

_DTYPE_ALIASES: Dict[str, str] = {
    "string": "string",
    "text": "string",
    "int64": "Int64",
    "int": "Int64",
    "float": "float64",
    "double": "float64",
    "bool": "boolean",
    "logical": "boolean",
}


def assert_columns(df: pd.DataFrame, expected: Sequence[str]) -> None:
    missing = [column for column in expected if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")


def _resolve_dtype(dtype: Any) -> Any:
    if isinstance(dtype, str):
        lowered = dtype.lower()
        if lowered in _DTYPE_ALIASES:
            return _DTYPE_ALIASES[lowered]
        return dtype
    return dtype


def coerce_types(df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    result = df.copy()
    for column, dtype in spec.items():
        if column not in result.columns:
            continue
        resolved = _resolve_dtype(dtype)
        if resolved in {"Int64", "int64"}:
            result[column] = pd.to_numeric(result[column], errors="coerce").astype(
                "Int64"
            )
        elif resolved in {"boolean", "bool"}:
            result[column] = (
                result[column]
                .replace({"true": True, "false": False, "True": True, "False": False})
                .astype("boolean")
            )
        elif resolved == "string":
            result[column] = result[column].astype("string")
        else:
            result[column] = result[column].astype(resolved)
    return result


def safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: Sequence[str],
    how: str = "left",
    suffixes: tuple[str, str] = ("_left", "_right"),
    validate: str | None = None,
) -> pd.DataFrame:
    logger.debug("Merging", extra={"on": on, "how": how})
    merged = left.merge(
        right, on=list(on), how=how, suffixes=suffixes, validate=validate
    )
    return merged


def deduplicate(df: pd.DataFrame, subset: Sequence[str]) -> pd.DataFrame:
    logger.debug("Deduplicating", extra={"subset": subset})
    return df.drop_duplicates(subset=list(subset))


def finalize_aggregate_columns(
    df: pd.DataFrame, columns: Iterable[str]
) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            continue
        result[column] = (
            pd.to_numeric(result[column], errors="coerce").fillna(0).astype("Int64")
        )
    return result


def sort_dataframe(df: pd.DataFrame, by: Sequence[str]) -> pd.DataFrame:
    missing = [column for column in by if column not in df.columns]
    if missing:
        raise ValueError(f"Cannot sort by missing columns: {missing}")
    return df.sort_values(by=list(by), kind="mergesort").reset_index(drop=True)


__all__ = [
    "assert_columns",
    "coerce_types",
    "safe_merge",
    "deduplicate",
    "finalize_aggregate_columns",
    "sort_dataframe",
]
