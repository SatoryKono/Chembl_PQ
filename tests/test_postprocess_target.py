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
            "recommended_name": ["Protein Alpha", "Existing Beta"],
            "gene_name": ["gene1", "betagene"],
            "synonyms": [
                "alpha canonical|alpha pref|alt a|altalpha|ga|gene1|subunit alpha",
                "beta canonical|beta pref",
            ],
            "protein_class_pred_L1": ["Enzyme", "Enzyme"],
            "protein_class_pred_L2": ["Transferase", "Multifunctional"],
            "protein_class_pred_L3": ["Kinase", pd.NA],
            "protein_class_pred_rule_id": ["IUPHAR_TYPE", "EC_MAJOR_MULTI"],
            "protein_class_pred_evidence": ["iuphar_type", "reaction_ec_numbers"],
            "protein_class_pred_confidence": ["1.0", "0.6"],
            "taxon_id": [9606, 10090],
            "lineage_superkingdom": ["Eukaryota", "Eukaryota"],
            "lineage_phylum": ["Chordata", "Streptophyta"],
            "lineage_class": ["Mammalia", pd.NA],
            "reaction_ec_numbers": ["2.7.11.1", "1.1.1.1|2.7.11.1"],
            "cellularity": ["multicellular", "multicellular"],
            "multifunctional_enzyme": ["false", "true"],
            "iuphar_name": ["IUPHAR A", pd.NA],
            "iuphar_target_id": ["GT1", pd.NA],
            "iuphar_family_id": ["GF1", pd.NA],
            "iuphar_type": ["Enzyme.Transferase", pd.NA],
            "iuphar_class": ["Enzyme", pd.NA],
            "iuphar_subclass": ["Transferase", pd.NA],
            "iuphar_chain": ["Kinase", pd.NA],
            "iuphar_full_id_path": ["pathA", pd.NA],
            "iuphar_full_name_path": ["nameA", pd.NA],
        }
    )

    type_map = test_config["pipeline"]["target"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)


def test_target_postprocess_missing_columns(test_config) -> None:
    minimal = pd.DataFrame(
        {
            "target_chembl_id": ["T1"],
            "uniprot_id_primary": ["P12345"],
        }
    )

    inputs = {"target": minimal}

    result = run(inputs, test_config)

    output_columns = test_config["pipeline"]["target"]["output_columns"]
    expected = pd.DataFrame([{column: pd.NA for column in output_columns}])
    expected.loc[0, "target_chembl_id"] = "T1"
    expected.loc[0, "uniprot_id_primary"] = "P12345"
    expected.loc[0, "synonyms"] = ""
    expected.loc[0, "reaction_ec_numbers"] = ""
    expected.loc[0, "cellularity"] = "ambiguous"
    expected.loc[0, "multifunctional_enzyme"] = "false"

    type_map = test_config["pipeline"]["target"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)
