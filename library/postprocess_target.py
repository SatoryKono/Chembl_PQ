from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from .transforms import clean_pipe
from .utils import coerce_types, deduplicate

logger = logging.getLogger(__name__)


def run(inputs: Dict[str, pd.DataFrame], config: dict) -> pd.DataFrame:
    target_df = inputs["target"].copy()

    logger.info("Starting target post-processing", extra={"rows": len(target_df)})

    if "synonyms" in target_df.columns:
        target_df["synonyms"] = clean_pipe(
            target_df["synonyms"],
            alias_map=None,
            drop_list=None,
            sort=config.get("cleaning", {}).get("sort_pipes", True),
        )

    type_map = config.get("pipeline", {}).get("target", {}).get("type_map", {})
    typed = coerce_types(target_df, type_map)

    output_columns = (
        config.get("pipeline", {}).get("target", {}).get("output_columns", [])
    )
    if output_columns:
        missing = [col for col in output_columns if col not in typed.columns]
        if missing:
            raise ValueError(f"Target output is missing columns: {missing}")
        typed = typed.loc[:, output_columns]

    if "target_chembl_id" in typed.columns:
        typed = deduplicate(typed, ["target_chembl_id"])

    return typed


__all__ = ["run"]
