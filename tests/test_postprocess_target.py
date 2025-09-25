from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from library.postprocess_target import run
from library.utils import coerce_types


def test_target_postprocess(target_inputs, test_config) -> None:
    result = run(target_inputs, test_config)

    expected = pd.DataFrame(
        {

            "target_chembl_id": ["T1", "T2", "T3"],
            "uniprot_id_primary": ["P12345", "P67890", "P11111"],
            "recommended_name": [
                "Protein Alpha",
                "Existing Beta",
                "Gamma Receptor",
            ],
            "gene_name": ["gene1", "betagene", "gammagene"],
            "synonyms": [
                "alpha canonical|alpha pref|altalpha|alt a|ga|gene1|subunit alpha",
                "beta canonical|beta pref",
                "gamma canonical|gamma alt|gg|gg1|gamma subunit",
            ],
            "protein_class_pred_L1": [
                "Enzyme",
                "Enzyme",
                "Transcription factor",
            ],
            "protein_class_pred_L2": [
                "Transferase",
                "Multifunctional",
                "Zinc finger",
            ],
            "protein_class_pred_L3": ["Kinase", "Multifunctional", "Zinc finger"],
            "protein_class_pred_rule_id": [
                "IUPHAR_TYPE",
                "EC_MAJOR_MULTI",
                "IUPHAR_TYPE",
            ],
            "protein_class_pred_evidence": [
                "iuphar_type",
                "reaction_ec_numbers",
                "iuphar_type",
            ],
            "protein_class_pred_confidence": ["1.0", "0.6", "1.0"],
            "taxon_id": [9606, 10090, 9606],
            "lineage_superkingdom": ["Eukaryota", "Eukaryota", "Eukaryota"],
            "lineage_phylum": ["Chordata", "Streptophyta", "Chordata"],
            "lineage_class": ["Mammalia", pd.NA, "Mammalia"],
            "reaction_ec_numbers": [
                "2.7.11.1",
                "1.1.1.1|2.7.11.1",
                "",
            ],
            "cellularity": ["multicellular", "multicellular", "multicellular"],
            "multifunctional_enzyme": ["false", "true", "false"],
            "iuphar_name": ["IUPHAR A", pd.NA, "IUPHAR G"],
            "iuphar_target_id": ["GT1", pd.NA, "GT3"],
            "iuphar_family_id": ["GF1", pd.NA, "GF3"],
            "iuphar_type": [
                "Enzyme.Transferase",
                pd.NA,
                "Receptor.Nuclear Hormone Receptor",
            ],
            "iuphar_class": ["Enzyme", pd.NA, "Receptor"],
            "iuphar_subclass": ["Transferase", pd.NA, "Nuclear Hormone Receptor"],
            "iuphar_chain": ["Kinase", pd.NA, "Zinc finger"],
            "iuphar_full_id_path": ["pathA", pd.NA, "pathG"],
            "iuphar_full_name_path": ["nameA", pd.NA, "nameG"],

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


def test_target_protein_classification_fallback(test_config) -> None:
    minimal = pd.DataFrame(
        {
            "target_chembl_id": ["TX"],
            "uniprot_id_primary": ["PX"],
            "protein_classifications": [
                '[{"protein_classification": "Enzyme"},'
                '{"protein_classification": "Transferase"},'
                '{"protein_classification": "Kinase"}]'
            ],
        }
    )

    inputs = {"target": minimal}

    result = run(inputs, test_config)

    expected = pd.DataFrame(
        {
            "target_chembl_id": ["TX"],
            "uniprot_id_primary": ["PX"],
            "recommended_name": [pd.NA],
            "gene_name": [pd.NA],
            "synonyms": [""],
            "protein_class_pred_L1": ["Enzyme"],
            "protein_class_pred_L2": ["Transferase"],
            "protein_class_pred_L3": ["Kinase"],
            "protein_class_pred_rule_id": ["PROTEIN_CLASSIFICATION"],
            "protein_class_pred_evidence": ["protein_classifications"],
            "protein_class_pred_confidence": ["0.7"],
            "taxon_id": [pd.NA],
            "lineage_superkingdom": [pd.NA],
            "lineage_phylum": [pd.NA],
            "lineage_class": [pd.NA],
            "reaction_ec_numbers": [""],
            "cellularity": ["ambiguous"],
            "multifunctional_enzyme": ["false"],
            "iuphar_name": [pd.NA],
            "iuphar_target_id": [pd.NA],
            "iuphar_family_id": [pd.NA],
            "iuphar_type": [pd.NA],
            "iuphar_class": [pd.NA],
            "iuphar_subclass": [pd.NA],
            "iuphar_chain": [pd.NA],
            "iuphar_full_id_path": [pd.NA],
            "iuphar_full_name_path": [pd.NA],
        }
    )


    type_map = test_config["pipeline"]["target"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)
