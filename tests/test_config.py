from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from library.config import load_config


def test_load_config_handles_windows_paths(tmp_path: Path) -> None:
    config_text = dedent(
        r"""
        files:
          document_csv: "C:\projects\Chembl_PQ\data\input\document.csv"
        outputs:
          line_terminator: "\n"
        """
    )
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_text, encoding="utf-8")

    config = load_config(config_file)

    expected_path = r"C:\projects\Chembl_PQ\data\input\document.csv"
    assert config["files"]["document_csv"] == expected_path
    assert config["outputs"]["line_terminator"] == "\n"


def test_load_config_repairs_unescaped_backslashes(tmp_path: Path) -> None:
    config_text = dedent(
        r"""
        files:
          activity_csv: "data\input\activity.csv"
        """
    )
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_text, encoding="utf-8")

    config = load_config(config_file)

    assert config["files"]["activity_csv"] == r"data\input\activity.csv"


def test_load_config_raises_original_error(tmp_path: Path) -> None:
    config_text = "files: ["
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_text, encoding="utf-8")

    with pytest.raises(yaml.YAMLError):
        load_config(config_file)
