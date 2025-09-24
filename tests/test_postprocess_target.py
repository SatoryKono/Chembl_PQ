from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from library.postprocess_target import run
from library.utils import coerce_types


def test_target_postprocess(target_inputs, test_config) -> None:
    result = run(target_inputs, test_config)

    expected = pd.DataFrame(
        {
            "target_chembl_id": ["T1", "T2"],
            "uniprot_id_primary": ["P12345", "P67890"],
            "recommended_name": ["Protein A", "Protein B"],
            "gene_name": ["GENE1", "GENE2"],
            "synonyms": ["syn1|syn2", ""],
            "protein_class_pred_L1": ["class1", "class4"],
            "protein_class_pred_L2": ["class2", "class5"],
            "protein_class_pred_L3": ["class3", "class6"],
            "protein_class_pred_rule_id": ["rule1", "rule2"],
            "protein_class_pred_evidence": ["evidence1", "evidence2"],
            "protein_class_pred_confidence": ["0.9", "0.8"],
            "taxon_id": [9606, 10090],
            "lineage_superkingdom": ["Eukaryota", "Eukaryota"],
            "lineage_phylum": ["Chordata", "Chordata"],
            "lineage_class": ["Mammalia", "Mammalia"],
            "reaction_ec_numbers": ["1.1.1.1", "2.2.2.2"],
            "cellularity": ["cellular", "acellular"],
            "multifunctional_enzyme": ["yes", "no"],
            "iuphar_name": ["IUPHAR A", "IUPHAR B"],
            "iuphar_target_id": ["GT1", "GT2"],
            "iuphar_family_id": ["GF1", "GF2"],
            "iuphar_type": ["typeA", "typeB"],
            "iuphar_class": ["classA", "classB"],
            "iuphar_subclass": ["subclassA", "subclassB"],
            "iuphar_chain": ["chainA", "chainB"],
            "iuphar_full_id_path": ["pathA", "pathB"],
            "iuphar_full_name_path": ["nameA", "nameB"],
        }
    )

    type_map = test_config["pipeline"]["target"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)
