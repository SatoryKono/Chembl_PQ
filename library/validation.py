from __future__ import annotations

import logging
from typing import Iterable, Sequence

import pandas as pd

logger = logging.getLogger(__name__)


def ensure_no_duplicates(df: pd.DataFrame, subset: Sequence[str], context: str) -> None:
    duplicated = df.duplicated(subset=list(subset))
    if duplicated.any():
        rows = df[duplicated]
        details = rows[list(subset)].to_dict("records")
        raise ValueError(f"Duplicate keys detected in {context}: {details}")


def ensure_not_null(df: pd.DataFrame, columns: Iterable[str], context: str) -> None:
    for column in columns:
        if column not in df.columns:
            continue
        if df[column].isna().any():
            raise ValueError(f"Null values found in column '{column}' for {context}")


def ensure_sorted(df: pd.DataFrame, by: Sequence[str], context: str) -> None:
    if df.empty:
        return
    sorted_df = df.sort_values(by=list(by), kind="mergesort").reset_index(drop=True)
    if not df[list(by)].equals(sorted_df[list(by)]):
        logger.warning(
            "DataFrame is not sorted as requested", extra={"context": context}
        )


__all__ = ["ensure_no_duplicates", "ensure_not_null", "ensure_sorted"]
