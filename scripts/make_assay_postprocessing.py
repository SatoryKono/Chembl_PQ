from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict

import yaml

from library.loaders import read_csv, write_csv
from library.postprocess_assay import run as run_assay

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _load_config(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def main() -> None:
    parser = argparse.ArgumentParser(description="Assay post-processing pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--out", help="Override output path")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = _load_config(config_path)

    assay_df = read_csv("assay_csv", config)
    activity_df = read_csv("activity_csv", config)

    result = run_assay(
        {
            "assay": assay_df,
            "activity": activity_df,
        },
        config,
    )

    outputs_cfg = config.get("outputs", {})
    default_path = (
        Path(outputs_cfg.get("dir", "data/output")) / "assay_postprocessed.csv"
    )
    output_path = Path(args.out) if args.out else default_path

    write_csv(result, output_path, config)
    logging.info("Assay post-processing completed", extra={"output": str(output_path)})


if __name__ == "__main__":  # pragma: no cover
    main()
