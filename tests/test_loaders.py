from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd

from library import io as library_io


def _build_config(
    base_path: Path,
    file_name: str,
    *,
    extra_io: dict[str, Any] | None = None,
    extra_source: dict[str, Any] | None = None,
) -> dict:
    source_cfg = {"kind": "file", "base_path": str(base_path)}
    if extra_source:
        source_cfg.update(extra_source)
    io_config = {
        "source": source_cfg,
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
    if extra_io:
        io_config["io"].update(extra_io)
    return io_config


def test_read_csv(tmp_path: Path) -> None:
    sample_path = tmp_path / "input.csv"
    sample_path.write_text("col1,col2\n1,2\n", encoding="utf-8")
    config = _build_config(tmp_path, "input.csv")

    df = library_io.read_csv("sample", config)

    assert list(df.columns) == ["col1", "col2"]
    assert df.iloc[0, 0] == 1


def test_read_csv_uses_fallback_directory(tmp_path: Path) -> None:
    primary_dir = tmp_path / "primary"
    fallback_dir = tmp_path / "fallback"
    primary_dir.mkdir()
    fallback_dir.mkdir()
    sample_path = fallback_dir / "input.csv"
    sample_path.write_text("col1\n1\n", encoding="utf-8")
    config = _build_config(
        primary_dir,
        "input.csv",
        extra_source={"fallback_dirs": [str(fallback_dir)]},
    )

    df = library_io.read_csv("sample", config)

    assert list(df.columns) == ["col1"]
    assert df.iloc[0, 0] == 1


def test_read_csv_falls_back_to_latest_dated_file(tmp_path: Path) -> None:
    primary_dir = tmp_path / "primary"
    fallback_dir = tmp_path / "fallback"
    primary_dir.mkdir()
    fallback_dir.mkdir()
    older_file = fallback_dir / "output.targets_20240101.csv"
    newer_file = fallback_dir / "output.targets_20240215.csv"
    older_file.write_text("col1\n1\n", encoding="utf-8")
    newer_file.write_text("col1\n2\n", encoding="utf-8")
    config = _build_config(
        primary_dir,
        "output.targets_20240301.csv",
        extra_source={"fallback_dirs": [str(fallback_dir)]},
    )

    df = library_io.read_csv("sample", config)

    assert list(df.columns) == ["col1"]
    assert df.iloc[0, 0] == 2


def test_read_csv_sets_low_memory(monkeypatch, tmp_path: Path) -> None:
    sample_path = tmp_path / "input.csv"
    sample_path.write_text("col1\n1\n", encoding="utf-8")
    config = _build_config(tmp_path, "input.csv")

    captured: dict[str, Any] = {}

    def fake_read_csv(path: Path, **kwargs):  # type: ignore[override]
        captured["kwargs"] = kwargs
        return pd.DataFrame({"col1": [1]})

    monkeypatch.setattr(library_io.pd, "read_csv", fake_read_csv)

    library_io.read_csv("sample", config)

    assert captured["kwargs"]["low_memory"] is False


def test_read_csv_with_encoding_fallback(tmp_path: Path) -> None:
    sample_path = tmp_path / "input_cp1251.csv"
    raw_text = "col1,тест\n1,привет\n"
    sample_path.write_bytes(raw_text.encode("cp1251"))
    config = _build_config(
        tmp_path,
        "input_cp1251.csv",
        extra_io={"encoding_fallbacks": ["cp1251"]},
    )

    df = library_io.read_csv("sample", config)

    assert "тест" in df.columns
    assert df.at[0, "тест"] == "привет"


def test_write_csv(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    config = _build_config(tmp_path, "output.csv")
    output_path = tmp_path / "output.csv"

    library_io.write_csv(df, output_path, config)

    with output_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        assert header == ["a", "b"]
