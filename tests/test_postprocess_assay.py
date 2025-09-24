from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from library.postprocess_assay import run
from library.utils import coerce_types


def test_assay_postprocess(assay_inputs, test_config) -> None:
    result = run(assay_inputs, test_config)

    expected = pd.DataFrame(
        {
            "assay_chembl_id": ["A1", "A2"],
            "document_chembl_id": ["DOC1", "DOC2"],
            "target_chembl_id": ["T1", "T2"],
            "assay_category": ["cat1", "cat2"],
            "assay_group": ["grp1", "grp2"],
            "assay_type": ["type1", "type2"],
            "assay_type_description": ["desc1", "desc2"],
            "assay_organism": ["organism1", "organism2"],
            "assay_test_type": ["test1", "test2"],
            "assay_cell_type": ["cell1", "cell2"],
            "assay_tissue": ["tissue1", "tissue2"],
            "assay_with_same_target": [1, 2],
            "bao_format": ["format1", "format2"],
            "bao_label": ["label1", "label2"],
            "aidx": ["aidx1", "aidx2"],
            "assay_classifications": ["class1|class2", "class3"],
            "assay_parameters": ["param1", "param2"],
            "assay_subcellular_fraction": ["fraction1", "fraction2"],
            "cell_chembl_id": ["CELL1", "CELL2"],
            "description": ["Description A", "Description B"],
            "src_assay_id": ["SA1", "SA2"],
            "src_id": [10, 20],
            "tissue_chembl_id": ["TISS1", "TISS2"],
            "variant_sequence": ["SEQ1", "SEQ2"],
            "document_assay_total": [2, 1],
        }
    )

    type_map = test_config["pipeline"]["assay"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)
