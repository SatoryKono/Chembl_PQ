# Configuration reference

The pipeline is driven entirely by ``config.yaml``. All parameters that were
previously encoded in Power Query are exposed as declarative options. The YAML
file is split into reusable sections:

## ``source``

Defines how inputs are accessed.

| Key | Type | Description |
| --- | --- | --- |
| ``kind`` | ``file`` \| ``http`` \| ``sharepoint`` | Mode used to resolve paths. |
| ``base_path`` | string | Base directory for ``file`` sources. |
| ``http_base`` | string | Base URL for ``http`` sources. |
| ``sharepoint.site_url`` | string | Root site used when ``kind = sharepoint``. |
| ``sharepoint.library`` | string | Optional SharePoint library name. |
| ``sharepoint.auth`` | ``env`` \| ``device`` \| ``secrets`` | Authentication flow identifier. |

## ``files``

Logical names mapped to input files. All CLI utilities look up paths by key
(e.g. ``document_csv``). Paths are relative to ``source.base_path``.

## ``io``

Input/output formatting options.

| Key | Description |
| --- | --- |
| ``encoding_in`` / ``encoding_out`` | Character encodings for reading and writing. |
| ``delimiter`` | Field delimiter used by CSV readers and writers. |
| ``quoting`` | Quoting strategy (``minimal``, ``all`` or ``none``). |
| ``na_values`` | Values interpreted as missing data. |

## ``options``

Runtime toggles used by the processing modules. ``strict_types`` enforces type
casts, ``preserve_order`` keeps the final column order deterministic and
``fail_on_schema_mismatch`` raises when required columns are absent.

## ``cleaning``

Global helpers for text normalisation. ``sort_pipes`` controls whether the
``clean_pipe`` helper sorts items within pipe-delimited fields. Optional
``alias_maps`` and ``drop_lists`` can point to external CSV files that map
taxonomy values.

## ``pipeline``

Per-output settings. Each subsection (``document``, ``testitem``, ``assay``,
``target``) contains:

* ``classification_rules`` – column specific alias/drop rules for pipe values.
* ``review`` – voting behaviour for the document review flag.
* ``invalid_rules`` / ``chirality_reference`` – business logic for test items.
* ``drop_columns`` – columns removed from the assay dataset.
* ``type_map`` – canonical pandas dtypes for every published column.
* ``column_order`` – final column ordering used by writers.
* ``output_columns`` – target schema (used by the target module).

## ``outputs``

Destination folder and formatting options for generated CSV files.
