from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from library import loaders


def _build_config(base_path: Path, file_name: str) -> dict:
    return {
        "source": {"kind": "file", "base_path": str(base_path)},
        "files": {"sample": file_name},
        "io": {
            "encoding_in": "utf8",
            "encoding_out": "utf8",
            "delimiter": ",",
            "quoting": "minimal",
            "na_values": ["", "NA", "null"],
        },
        "outputs": {"dir": str(base_path), "line_terminator": "\n"},
    }


def test_read_csv(tmp_path: Path) -> None:
    sample_path = tmp_path / "input.csv"
    sample_path.write_text("col1,col2\n1,2\n", encoding="utf-8")
    config = _build_config(tmp_path, "input.csv")

    df = loaders.read_csv("sample", config)

    assert list(df.columns) == ["col1", "col2"]
    assert df.iloc[0, 0] == 1


def test_write_csv(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    config = _build_config(tmp_path, "output.csv")
    output_path = tmp_path / "output.csv"

    loaders.write_csv(df, output_path, config)

    with output_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        assert header == ["a", "b"]
