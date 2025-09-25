# Output datasets

All post-processing functions return fully typed pandas ``DataFrame`` objects.
Columns, ordering and data types are enforced by ``pipeline.*.type_map`` and
``pipeline.*.column_order`` in ``config.yaml``.

## Document

* Primary key: ``ChEMBL.document_chembl_id``
* Column highlights: review voting (``review``/``is_experimental``) and activity
  aggregates (``n_activity``, ``n_assay``, ``n_testitem``).
* Types: nullable integers use ``Int64`` (pandas extension type) to avoid float
  placeholders.

## Test item

* Primary key: ``molecule_chembl_id``.
* Enrichments: ``invalid_record`` (quality flag driven by the config rules).
* ``unknown_chirality`` converts the Power Query logic to boolean flags.
* ``canonical_smiles`` is sourced directly from the raw ``testitem_csv`` for
  downstream structure matching.

## Assay

* Primary key: ``assay_chembl_id``.
* Derived column ``document_assay_total`` counts distinct assays per document.
* Noise columns (tax IDs, confidence descriptors, relationship fields) are
  dropped via ``pipeline.assay.drop_columns``.

## Target

* Primary key: ``target_chembl_id``.
* ``synonyms`` normalised using the shared ``clean_pipe`` helper and deduplicated
  by target identifier.
* Outputs strictly match ``pipeline.target.output_columns`` with types forced via
  ``pipeline.target.type_map``.
