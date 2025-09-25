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

from library.config import load_config
from library.io import read_csv, write_csv
from library.transforms.activity import normalize_activity_frame

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def get_activity_data(config: Dict[str, object]) -> pd.DataFrame:
    activity_df = read_csv("activity_csv", config)
    pipeline_cfg = config.get("pipeline", {}).get("activity", {})
    type_map = pipeline_cfg.get("type_map", {})
    normalized = normalize_activity_frame(activity_df, type_map)
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(description="Activity retrieval pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--out", help="Override output path")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)

    result = get_activity_data(config)

    outputs_cfg = config.get("outputs", {})
    default_path = Path(outputs_cfg.get("dir", "data/output")) / "activity_postprocessed.csv"
    output_path = Path(args.out) if args.out else default_path

    write_csv(result, output_path, config)
    logging.info("Activity data export completed", extra={"output": str(output_path)})


if __name__ == "__main__":  # pragma: no cover
    main()
