from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> None:
    from library.config import load_config
    from library.loaders import read_csv, write_csv
    from library.postprocess_document import run as run_document

    parser = argparse.ArgumentParser(description="Document post-processing pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--out", help="Override output path")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    document_df = read_csv("document_csv", config)

    files_cfg = config.get("files", {})
    document_out_key = "document_csv_out" if "document_csv_out" in files_cfg else "document_out_csv"
    document_out_df = read_csv(document_out_key, config)

    document_ref_key = (
        "document_reference_csv"
        if "document_reference_csv" in files_cfg
        else "document_csv"
    )
    document_ref_df = read_csv(document_ref_key, config)
    activity_df = read_csv("activity_csv", config)
    citation_df = read_csv("citation_fraction_csv", config)

    result = run_document(
        {
            "document": document_df,
            "document_out": document_out_df,
            "document_reference": document_ref_df,
            "activity": activity_df,
            "citation_fraction": citation_df,
        },
        config,
    )

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
