# Руководство по использованию

## Предварительные требования

* Python версии 3.10 и выше.
* Зависимости из `requirements.txt` (`pandas`, `pyyaml`, `pytest`).

Установите инструменты в рабочее окружение:

```bash
python -m pip install -r requirements.txt
```

## Запуск конвейеров

Для каждого набора данных предусмотрен отдельный CLI в каталоге `scripts/`.
Передайте путь к `config.yaml` и при необходимости укажите выходной файл через
параметр `--out`.

```bash
python scripts/make_document_postprocessing.py --config config.yaml
python scripts/make_testitem_postprocessing.py --config config.yaml
python scripts/make_assay_postprocessing.py --config config.yaml
python scripts/make_target_postprocessing.py --config config.yaml
```

Если `--out` не указан, результаты сохраняются в каталог `outputs.dir`,
определенный в конфигурации. При повторном запуске на те же файлы установите
`outputs.overwrite` в `true`.

## Запуск тестов

Выполните модульные тесты командой `pytest`:

```bash
pytest -q
```

Тесты используют `tests/data/test_config.yaml` и легковесные CSV-файлы для
проверки схем, порядка столбцов и производных метрик.

## Устранение неполадок

* `LoaderError: File not found` — проверьте `source.base_path` и соответствующие
  ключи раздела `files` в `config.yaml`.
* `ValueError: ... missing columns` — один из входных файлов не содержит
  обязательного столбца. Сверьтесь со схемой из `docs/OUTPUT_RU.md`.
* Ошибки приведения типов — пересмотрите `pipeline.*.type_map` и убедитесь, что
  исходные данные приводятся к заявленным типам.
