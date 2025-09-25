from __future__ import annotations

import pandas as pd
import pandas.testing as pdt

from library.postprocess_document import run
from library.utils import coerce_types


def test_document_postprocess(document_inputs, test_config) -> None:
    result = run(document_inputs, test_config)

    expected = pd.DataFrame(
        {
            "PMID": [1001, 1002],
            "doi": ["10.1000/xyz", "10.1000/abc"],
            "sort_order": [
                "1234-5678:2020-01-01:00001001",
                "8765-4321:2021-06-01:00001002",
            ],
            "completed": ["2020-01-01", "2021-06-01"],
            "invalid_record": [False, False],
            "title": ["Title A", "Title B"],
            "abstract": ["Abstract A", "Abstract B"],
            "authors": ["Author One|Author Two", "Author Three"],
            "PubMed.MeSH": ["Term1|Term2", "Term3|Term4"],
            "OpenAlex.MeSH": ["OA1|OA2", "OA3"],
            "MeSH.qualifiers": ["Qual1", "Qual2"],
            "chemical_list": ["Chem1", "Chem2"],
            "ChEMBL.document_chembl_id": ["DOC1", "DOC2"],
            "PubMed.publication_type": ["review", "clinical trial"],
            "scholar.PublicationTypes": ["review", ""],
            "OpenAlex.publication_type": ["", ""],
            "OpenAlex.crossref_type": ["", ""],
            "OpenAlex.Genre": ["", ""],
            "crossref.publication_type": ["", ""],
            "significant_citations_fraction": [False, True],
            "document_contains_external_links": [True, False],
            "is_experimental_doc": [False, True],
            "n_activity": [2, 1],
            "citations": [1, 1],
            "n_assay": [2, 1],
            "n_testitem": [2, 1],
            "n_responces": [4, 3],
            "review": [True, False],
            "is_experimental": [False, True],
        }
    )

    type_map = test_config["pipeline"]["document"]["type_map"]
    expected = coerce_types(expected, type_map)

    pdt.assert_frame_equal(result, expected)


def test_document_postprocess_backfills_missing_columns(
    document_inputs, test_config
) -> None:
    minimal_document = document_inputs["document"][
        ["ChEMBL.document_chembl_id"]
    ].copy()
    minimal_document_out = document_inputs["document_out"][
        ["ChEMBL.document_chembl_id"]
    ].copy()
    inputs = {
        "document": minimal_document,
        "document_out": minimal_document_out,
        "activity": document_inputs["activity"],
        "citation_fraction": document_inputs["citation_fraction"],
    }

    result = run(inputs, test_config)

    column_order = test_config["pipeline"]["document"]["column_order"]
    assert list(result.columns) == column_order

    type_map = test_config["pipeline"]["document"]["type_map"]
    expected = coerce_types(result, type_map)
    pdt.assert_frame_equal(result, expected)
