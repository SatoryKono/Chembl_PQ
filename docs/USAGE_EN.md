# Usage guide

## Prerequisites

* Python 3.10 or later.
* Dependencies listed in `requirements.txt` (`pandas`, `pyyaml`, `pytest`).

Install the toolchain into your environment:

```bash
python -m pip install -r requirements.txt
```

## Running the pipelines

Each dataset has a dedicated CLI stored under `scripts/`. Provide the path to
`config.yaml` and, optionally, override the destination file with `--out`.

```bash
python scripts/make_document_postprocessing.py --config config.yaml
python scripts/make_testitem_postprocessing.py --config config.yaml
python scripts/make_assay_postprocessing.py --config config.yaml
python scripts/make_target_postprocessing.py --config config.yaml
```

Unless `--out` is supplied the results are written to `outputs.dir` defined in
the configuration. Set `outputs.overwrite` to `true` when re-running on the same
filenames.

## Running the tests

Execute the unit suite with `pytest`:

```bash
pytest -q
```

The tests load `tests/data/test_config.yaml` together with lightweight CSV
fixtures to verify schemas, column ordering and derived metrics.

## Troubleshooting

* `LoaderError: File not found` – verify `source.base_path` and the entries under
  `files` in `config.yaml`.
* `ValueError: ... missing columns` – one of the inputs is missing a required
  column. Cross-check the expected schema in `docs/OUTPUT_EN.md`.
* Type coercion failures – review `pipeline.*.type_map` and make sure the raw
  data can be cast to the declared dtypes.
