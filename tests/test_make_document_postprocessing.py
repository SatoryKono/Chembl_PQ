from __future__ import annotations

import copy
from pathlib import Path

import pandas as pd

from library.config import load_config
from scripts.get_document_data import get_document_data


def test_get_document_data_loads_inputs() -> None:
    config = load_config(Path("tests/data/test_config.yaml"))

    data = get_document_data(config)

    expected_keys = {
        "document",
        "document_out",
        "document_reference",
        "activity",
        "citation_fraction",
    }
    assert set(data.keys()) == expected_keys
    assert all(isinstance(frame, pd.DataFrame) for frame in data.values())
    assert data["document_out"] is data["document"]
    assert list(data["citation_fraction"].columns) == [
        "N",
        "K_min_significant",
        "test_used_at_threshold",
        "p_value_at_threshold",
    ]

    excluded = {
        "document_contains_external_links",
        "is_experimental_doc",
        "citations",
        "n_responces",
        "significant_citations_fraction",
    }
    for key in ("document", "document_out", "document_reference"):
        assert excluded.isdisjoint(data[key].columns)


def test_get_document_data_uses_document_fallback() -> None:
    config = load_config(Path("tests/data/test_config.yaml"))
    config_fallback = copy.deepcopy(config)
    config_fallback["files"].pop("document_reference_csv")

    data = get_document_data(config_fallback)

    assert data["document_reference"] is data["document"]
    assert data["document_out"] is data["document"]
