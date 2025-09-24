from __future__ import annotations

import pandas as pd

from library.transforms import clean_pipe, to_text


def test_to_text_normalization() -> None:
    assert to_text(None) == ""
    assert to_text("  Value  ") == "Value"


def test_clean_pipe_applies_alias_and_drop() -> None:
    series = pd.Series(["Review | Journal", "Study|Article", None])
    alias = {"review": "review", "study": "analysis"}
    drop = ["journal", "article"]

    cleaned = clean_pipe(series, alias_map=alias, drop_list=drop, sort=True)

    assert cleaned.tolist() == ["review", "analysis", ""]
