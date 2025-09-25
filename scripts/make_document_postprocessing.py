from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from library.config import load_config
from library.loaders import LoaderError, read_csv, write_csv
from library.postprocess_document import run as run_document

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _resolve_path_key(
    files_cfg: Dict[str, str], primary: str, fallback: str | None = None
) -> str:
    if primary in files_cfg:
        return primary
    if fallback is not None:
        return fallback
    raise LoaderError(f"No configured path for '{primary}'")


def get_document_data(config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """Load all inputs required by the document pipeline."""

    files_cfg = config.get("files", {})
    cache: Dict[str, pd.DataFrame] = {}

    def load_dataset(path_key: str) -> pd.DataFrame:
        if path_key not in cache:
            cache[path_key] = read_csv(path_key, config)
        return cache[path_key]

    document_df = load_dataset("document_csv")

    document_out_key = _resolve_path_key(
        files_cfg, "document_csv_out", "document_out_csv"
    )
    document_out_df = load_dataset(document_out_key)

    document_reference_key = _resolve_path_key(
        files_cfg, "document_reference_csv", "document_csv"
    )
    document_reference_df = load_dataset(document_reference_key)

    activity_key = _resolve_path_key(
        files_cfg, "activity_reference_csv", "activity_csv"
    )
    activity_df = load_dataset(activity_key)

    citation_key = _resolve_path_key(
        files_cfg, "citation_fraction_csv", "citation_csv"
    )
    citation_df = load_dataset(citation_key)

    return {
        "document": document_df,
        "document_out": document_out_df,
        "document_reference": document_reference_df,
        "activity": activity_df,
        "citation_fraction": citation_df,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Document post-processing pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--out", help="Override output path")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    inputs = get_document_data(config)

    result = run_document(inputs, config)

    outputs_cfg = config.get("outputs", {})
    default_path = (
        Path(outputs_cfg.get("dir", "data/output")) / "document_postprocessed.csv"
    )
    output_path = Path(args.out) if args.out else default_path

    write_csv(result, output_path, config)
    logging.info(
        "Document post-processing completed", extra={"output": str(output_path)}
    )


if __name__ == "__main__":  # pragma: no cover
    main()
