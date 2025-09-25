# Configuration reference

The processing toolkit is fully driven by `config.yaml`. Every command-line
utility reads the same declarative settings, so keeping the file accurate is
critical. The configuration is split into reusable sections listed below.

## `source`

Connection settings that define how inputs are resolved.

| Key | Type | Description |
| --- | --- | --- |
| `kind` | `file` \| `http` \| `sharepoint` | Backend used for resolving paths. |
| `base_path` | string | Root directory for file-based sources. |
| `http_base` | string | Base URL applied to HTTP downloads. |
| `sharepoint.site_url` | string | SharePoint site that hosts the data. |
| `sharepoint.library` | string | Optional SharePoint document library. |
| `sharepoint.auth` | `env` \| `device` \| `secrets` | Authentication flow identifier. |

## `files`

Logical aliases mapped to physical files. Every loader and CLI looks up inputs by
key (for example `document_csv`). Paths are interpreted relative to
`source.base_path` when `kind = file`.

## `io`

Shared formatting defaults for all tabular readers and writers.

| Key | Description |
| --- | --- |
| `encoding_in` / `encoding_out` | Character encodings used during import/export. |
| `delimiter` | Field delimiter for CSV files. |
| `quoting` | Quoting strategy (`minimal`, `all` or `none`). |
| `na_values` | List of markers treated as missing values. |

## `options`

Runtime toggles that control type coercion and column order enforcement.

| Key | Description |
| --- | --- |
| `strict_types` | Enforce the schema declared by every module. |
| `preserve_order` | Keep column ordering deterministic across runs. |
| `fail_on_schema_mismatch` | Raise an error when required columns are absent. |
| `dateformat` | Canonical output format for date columns. |

## `cleaning`

Global helpers shared across modules. `alias_maps` and `drop_lists` point to
auxiliary CSV files that normalise controlled vocabularies, while `sort_pipes`
controls whether helper routines sort values inside pipe-delimited columns.

## `pipeline`

Per-output configuration. Each subsection (such as `document`, `testitem`,
`assay`, `activity`, `target`) can declare the following blocks:

* `classification_rules` – column-level alias and drop rules applied to
  pipe-delimited values before aggregation.
* `review` – scoring weights and thresholds that drive the document review flag.
* `rename_map` – optional mapping that standardises legacy column names.
* `invalid_rules` / `chirality_reference` – quality filters specific to test
  items and activities.
* `drop_columns` – columns removed prior to publishing.
* `type_map` – canonical pandas dtypes applied to every published column.
* `column_order` – final column ordering used by writers.
* `output_columns` – explicit schema declaration for downstream consumers.

## `outputs`

Destination and formatting settings for generated CSV files.

| Key | Description |
| --- | --- |
| `dir` | Default output directory for exported datasets. |
| `overwrite` | Allow replacing existing files when set to `true`. |
| `line_terminator` | Line ending used by CSV writers. |
