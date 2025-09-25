from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def test_config() -> Dict[str, object]:
    config_path = Path("tests/data/test_config.yaml")
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture()
def document_inputs(test_config: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    base = Path(test_config["source"]["base_path"])
    document_df = pd.read_csv(base / test_config["files"]["document_csv"])
    document_out_df = pd.read_csv(base / test_config["files"]["document_out_csv"])
    activity_df = pd.read_csv(base / test_config["files"]["activity_csv"])
    citation_df = pd.read_csv(base / test_config["files"]["citation_fraction_csv"])
    return {
        "document": document_df,
        "document_out": document_out_df,
        "activity": activity_df,
        "citation_fraction": citation_df,
    }


@pytest.fixture()
def testitem_inputs(test_config: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    base = Path(test_config["source"]["base_path"])
    testitem_df = pd.read_csv(base / test_config["files"]["testitem_csv"])
    testitem_reference_df = pd.read_csv(
        base / test_config["files"]["testitem_reference_csv"]
    )
    activity_df = pd.read_csv(base / test_config["files"]["activity_csv"])
    return {
        "testitem": testitem_df,
        "testitem_reference": testitem_reference_df,
        "activity": activity_df,
    }


@pytest.fixture()
def assay_inputs(test_config: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    base = Path(test_config["source"]["base_path"])
    assay_df = pd.read_csv(base / test_config["files"]["assay_csv"])
    activity_df = pd.read_csv(base / test_config["files"]["activity_csv"])
    return {"assay": assay_df, "activity": activity_df}


@pytest.fixture()
def target_inputs(test_config: Dict[str, object]) -> Dict[str, pd.DataFrame]:
    base = Path(test_config["source"]["base_path"])
    target_df = pd.read_csv(base / test_config["files"]["target_csv"])
    return {"target": target_df}
