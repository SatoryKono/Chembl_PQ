# Usage guide

## Requirements

* Python 3.10 or later
* ``pandas`` and ``pyyaml``

Install development dependencies:

```bash
pip install -r requirements.txt  # or pip install pandas pyyaml pytest
```

## Running the pipelines

Each output has an individual CLI located in ``scripts/``. Provide the path to
``config.yaml`` and optionally override the destination file with ``--out``.

```bash
python scripts/make_document_postprocessing.py --config config.yaml
python scripts/make_testitem_postprocessing.py --config config.yaml
python scripts/make_assay_postprocessing.py --config config.yaml
python scripts/make_target_postprocessing.py --config config.yaml
```

By default, results are written to ``outputs.dir`` specified in the config. Use
``--out`` to write to another file.

## Working with tests

Run the unit suite with ``pytest``:

```bash
pytest -q
```

The test harness uses ``tests/data/test_config.yaml`` with lightweight CSV
fixtures to validate data types, column order and derived metrics.

## Troubleshooting

* ``LoaderError: File not found`` – verify ``source.base_path`` and the entries
  under ``files`` in the configuration.
* ``ValueError: ... missing columns`` – one of the inputs is missing a required
  column. Check the schema documented in ``docs/OUTPUT.md``.
* Type coercion failures – review ``pipeline.*.type_map`` and ensure the raw
  data can be safely cast to the declared types.
