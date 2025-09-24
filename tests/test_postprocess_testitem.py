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
            "pref_name": ["Drug A", "Drug C"],
            "all_names": ["Drug A|Compound A", ""],
            "molecule_type": ["Small molecule", "Biologic"],
            "structure_type": ["MOL", "PROT"],
            "is_radical": [False, False],
            "standard_inchi_key": ["KEY1", ""],
            "unknown_chirality": [False, True],
            "document_chembl_id": ["DOC1", "DOC2"],
            "document_testitem_total": [2, 1],
            "invalid_record": [False, True],
        }
    )

    type_map = test_config["pipeline"]["testitem"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)
