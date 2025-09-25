from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _resolve_key(files_cfg: Dict[str, str], *candidates: str) -> str:
    for key in candidates:
        if key in files_cfg:
            return key
    if candidates:
        return candidates[-1]
    raise ValueError("No candidates provided for key resolution")


LOGGER = logging.getLogger(__name__)


def get_document_data(config: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    from library.loaders import LoaderError, read_csv


    files_cfg = config.get("files", {})
    if not isinstance(files_cfg, dict):  # pragma: no cover - defensive
        raise TypeError("config['files'] must be a mapping")

    document_df = read_csv("document_csv", config)

    document_ref_key = _resolve_key(files_cfg, "document_reference_csv", "document_csv")
    document_out_key  = document_ref_key
    activity_ref_key = _resolve_key(files_cfg, "activity_reference_csv", "activity_csv")
    citation_key = _resolve_key(
        files_cfg, "citation_reference_csv", "citation_fraction_csv", "citation_csv"
    )

    if document_ref_key == "document_csv":
        document_ref_df = document_df
    else:
        document_ref_df = read_csv(document_ref_key, config)

    if document_out_key == document_ref_key:
        document_out_df = document_ref_df
    else:

        try:
            document_out_df = read_csv(document_out_key, config)
        except LoaderError as error:
            if "File not found" not in str(error):
                raise
            LOGGER.warning(
                "Document output CSV missing; falling back to reference dataset",
                extra={
                    "requested_key": document_out_key,
                    "fallback_key": document_ref_key,
                },
            )
            document_out_df = document_ref_df


    activity_df = read_csv(activity_ref_key, config)

    citation_df = read_csv(citation_key, config)

    return {
        "document": document_df,
        "document_out": document_out_df,
        "document_reference": document_ref_df,
        "activity": activity_df,
        "citation_fraction": citation_df,
    }


def main() -> None:
    from library.config import load_config
    from library.loaders import write_csv
    from library.postprocess_document import run as run_document

    parser = argparse.ArgumentParser(description="Document post-processing pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--out", help="Override output path")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    data_frames = get_document_data(config)

    result = run_document(data_frames, config)

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
