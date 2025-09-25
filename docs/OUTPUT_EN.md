# Output datasets

Every post-processing CLI returns a fully typed pandas `DataFrame`. Column names,
ordering and dtypes are enforced with the `pipeline.*.type_map` and
`pipeline.*.column_order` entries in `config.yaml`. The exported CSV files share
common defaults declared in the `outputs` section (directory, overwrite policy
and line terminator).

## Document

* Primary key: `ChEMBL.document_chembl_id`.
* Aggregated metrics: activity, assay and test item counts (`n_activity`,
  `n_assay`, `n_testitem`).
* Quality signals: document review vote (`review`) and experimental flag
  (`is_experimental`).
* Text fields are lower-cased and pipe-delimited values are normalised through
  the shared cleaning helpers.

## Test item

* Primary key: `molecule_chembl_id`.
* Derived flags: `invalid_record` (quality rules) and `unknown_chirality`
  (boolean mirror of the legacy Power Query logic).
* Structural data: `canonical_smiles` is propagated untouched for downstream
  structure matching.
* Activity joins use the prepared activity dataset to ensure consistent counts.

## Assay

* Primary key: `assay_chembl_id`.
* Derived column `document_assay_total` counts distinct assays per document.
* Noise columns (taxonomy identifiers, relationship flags) are removed using the
  configured `drop_columns` list.
* Activity aggregates mirror the calculations used by the document module.

## Target

* Primary key: `target_chembl_id`.
* Synonyms are normalised with the shared `clean_pipe` helper and deduplicated by
  target identifier.
* Classification fields (`protein_class_pred_*`) are aligned to the configured
  schema via `type_map`.
* The exported schema strictly matches `pipeline.target.output_columns` to keep
  downstream integrations stable.
