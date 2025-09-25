from __future__ import annotations

from typing import Mapping, MutableMapping

import pandas as pd

from ..validators import coerce_types


def normalize_activity(record: Mapping[str, object]) -> dict[str, object]:
    """Return a mutable copy of *record* for downstream processing."""

    return dict(record)


def normalize_activity_frame(
    frame: pd.DataFrame, type_map: Mapping[str, object] | None = None
) -> pd.DataFrame:
    """Apply :func:`normalize_activity` row-wise and coerce configured types."""

    normalized_rows: list[MutableMapping[str, object]] = [
        normalize_activity(row)
        for row in frame.to_dict(orient="records")
    ]
    normalized = pd.DataFrame(normalized_rows)
    if type_map:
        normalized = coerce_types(normalized, dict(type_map))
    return normalized


__all__ = ["normalize_activity", "normalize_activity_frame"]
