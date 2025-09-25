from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from library.postprocess_testitem import run
from library.utils import coerce_types


def test_testitem_postprocess(testitem_inputs, test_config) -> None:
    result = run(testitem_inputs, test_config)

    expected = pd.DataFrame(
        {
            "molecule_chembl_id": ["M1", "M3"],
            "pref_name": ["drug a", "drug c"],
            "all_names": ["compound a|drug a", pd.NA],
            "standard_inchi_key": ["KEY1", pd.NA],
            "canonical_smiles": ["C1=CC=CC=C1", pd.NA],
            "skeleton_inchi_key": ["KEY1", pd.NA],
            "unknown_chirality": [False, True],
            "invalid_record": [False, True],
        }
    )

    type_map = test_config["pipeline"]["testitem"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)
