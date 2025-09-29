# Постобработка целей ChEMBL

Этот документ описывает поведение скрипта `scripts/get_target_data.py` и модулей `library/transforms/target.py` на основе анализа текущей версии репозитория. Материал систематизирует источники данных, этапы пайплайна, правила нормализации и конфигурацию, применяемые при подготовке набора целей ChEMBL.

## 1. Точки входа и зависимости
- **Запуск**: функция `main()` в `scripts/get_target_data.py` загружает YAML-конфиг, читает входной CSV и запускает `normalize_target` перед записью результата.
- **Ключевые модули**: `library.config.load_config`, `library.io.read_csv`, `library.io.write_csv`, `library.transforms.target.normalize_target`.
- **Библиотеки**: `pandas`, `yaml`, `argparse`, `logging`, `pathlib`.

## 2. Входные источники
- **Файл**: `files.target_csv` из конфига (`source.base_path` + `io.*` параметры).
- **Формат**: CSV с разделителем/кодировкой из `io.delimiter`, `io.encoding_in`, `io.na_values` и др.
- **Колонки**: идентификаторы целей (`target_chembl_id`, `uniprot_id_primary`), имена (`recommended_name`, `pref_name`, `protein_name_*`, `gene_*`), компоненты (`target_components`), классификация (`protein_classifications`, `protein_class_pred_*`), таксономия (`taxon_id`, `lineage_*`), EC (`reaction_ec_numbers`), IUPHAR (`iuphar_*`).
- **Пример**: `tests/data/target.csv` содержит минимальные записи для проверки логики.

## 3. Карта пайплайна (S1…S9)
| Стадия | Описание | Ссылки |
|--------|----------|--------|
| S1 | Загрузка конфига и аргументов | `scripts/get_target_data.py` 19–39 |
| S2 | Чтение входного CSV | `library/io.py` 167–218 |
| S3 | Обогащение имён, синонимов | `library/transforms/target.py` 269–328 |
| S4 | Нормализация EC, клеточности | `library/transforms/target.py` 331–360 |
| S5 | Белковая классификация | `library/transforms/target.py` 364–577 |
| S6 | Финальная чистка синонимов | `library/transforms/target.py` 242–248 |
| S7 | Гарантия колонок и типов | `library/transforms/target.py` 250–262; `library/validators.py` 37–77 |
| S8 | Дедупликация | `library/transforms/target.py` 263–266 |
| S9 | Запись результата | `scripts/get_target_data.py` 32–39; `library/io.py` 227–250 |

## 4. Линейдж колонок
| OutputColumn | Тип | Source(s) | Трансформация | Nullable | Валидация | Пример |
|--------------|-----|-----------|---------------|----------|-----------|--------|
| target_chembl_id | string | `target.target_chembl_id` | Прямое копирование | false | Дедуп по ключу | `T1 → T1` |
| uniprot_id_primary | string | `target.uniprot_id_primary` | Прямое копирование | false | Строковый dtype | `P12345 → P12345` |
| recommended_name | string | `recommended_name`, `recommendedName`, `pref_name`, `protein_name_canonical` | Первый непустой | true | Trim | `""/Protein Alpha → Protein Alpha` |
| gene_name | string | `gene_name`, `geneName`, `gene_symbol_list` | Первый токен → lower | true | Очистка скобок | `BetaGene → betagene` |
| synonyms | string | `protein_name_canonical`, `pref_name`, `protein_name_alt`, `gene_symbol_list`, `target_components` | Конкатенация, regex описаний, `clean_pipe(sort=False)` | true | Удаление пустых токенов | `Alpha + {...Subunit Alpha} → alpha canonical\|...` |
| protein_class_pred_L1/L2/L3 | string | `protein_class_pred_*`, IUPHAR, `protein_classifications`, EC | Приоритеты IUPHAR → JSON → EC | true | `CLASS_LABEL_MAP` | `"" → Enzyme (EC multi)` |
| protein_class_pred_rule_id | string | `protein_class_pred_rule_id` или идентификатор правила | true | Источник правила | `"" → EC_MAJOR_MULTI` |
| protein_class_pred_evidence | string | `protein_class_pred_evidence` или поле-источник | true | Строковый dtype | `"" → reaction_ec_numbers` |
| protein_class_pred_confidence | string | `protein_class_pred_confidence` или оценка правила | true | Значения `0.6/0.7/1.0` | `"" → 0.6` |
| taxon_id | Int64 | `target.taxon_id` | `to_numeric` | true | Int64 | `10090 → 10090` |
| lineage_superkingdom/phylum/class | string | `target.lineage_*` | Прямое копирование | true | Строки | `Eukaryota → Eukaryota` |
| reaction_ec_numbers | string | `target.reaction_ec_numbers` | `_split_pipe` → `_join_pipe_tokens` | true | Пустые → `""` | `1.1.1.1\|2.7.11.1 → 1.1.1.1\|2.7.11.1` |
| cellularity | string | `lineage_superkingdom`, `lineage_phylum`, `lineage_class` | Категоризация наборами таксонов | true | `{multicellular, unicellular, ...}` | `Eukaryota + Streptophyta → multicellular` |
| multifunctional_enzyme | string | `reaction_ec_numbers` | Подсчёт EC major (>1, без «3») | true | `true/false` | `1.1.1.1\|2.7.11.1 → true` |
| iuphar_* | string | `target.iuphar_*` | Прямое копирование (с нормализацией) | true | `_normalize_key` | `"" → <NA>` |

## 5. Джойны и ключи
Пайплайн работает в одном DataFrame, внешние `merge`/`join` не используются; связи компонент инкапсулированы в исходном CSV.

## 6. Нормализация и бизнес-правила
- Порядок имен: `recommended_name` → `recommendedName` → `pref_name` → `protein_name_canonical`.
- `gene_name`: fallback до `gene_symbol_list` с приводом к нижнему регистру.
- Синонимы: объединение текстовых полей + описаний компонентов, далее `clean_pipe` без сортировки (сохранение порядка, удаление дубликатов).
- EC: очистка, удаление пустых токенов, сортировка уникальных значений.
- `cellularity`: определение по множествам суперцарств/филумов, амбигуитет → `ambiguous`.
- `multifunctional_enzyme`: `true`, если >1 уникального EC major (кроме шумовых «3»).
- Белковая классификация: приоритет IUPHAR → JSON `protein_classifications` → правила по EC major с установкой `rule_id`, `evidence`, `confidence`.
- Пустые/`n/a` нормализуются в `<NA>`.
- `ensure_columns` и `coerce_types` гарантируют схему из `config.yaml`.

## 7. Фильтры, дедуп, сортировки
- Фильтров строк нет.
- `deduplicate(..., ["target_chembl_id"])` обеспечивает уникальность записей.
- Сортировка входных строк сохраняется; внутри `synonyms` порядок исходных токенов.

## 8. Валидация и обработка ошибок
- `load_config` сообщает о проблемах чтения YAML.
- `read_csv` поддерживает fallback-директории, перебор кодировок и осмысленные сообщения.
- `write_csv` создаёт каталог и проверяет quoting.
- Ошибки типов переводятся в `<NA>` посредством `coerce_types`.

## 9. Производительность
- Три строковых `apply` (имена, EC, классификация) — потенциальные узкие места; возможно уплотнение маппингами.
- Повторных чтений нет; используется один CSV.
- `clean_pipe` работает посерийно, но достаточно эффективно для объёмов ChEMBL.

## 10. Выходные артефакты
- Файл: `data/output/target_postprocessed.csv` (или путь `--out`).
- Формат: CSV c параметрами из `io.*`, без индекса.
- Колонки и порядок соответствуют `pipeline.target.output_columns`.

## 11. Конфигурация
- Используемые ключи: `source.kind`, `source.base_path`, `source.fallback_dirs`, `files.target_csv`, `io.encoding_in/out`, `io.delimiter`, `io.quoting`, `io.na_values`, `io.encoding_errors`, `outputs.dir`, `outputs.line_terminator`, `pipeline.target.output_columns`, `pipeline.target.type_map`.
- Alias и drop-листы таксономии пока не применяются в коде.

## 12. Тестовые примеры
- `tests/test_postprocess_target.py::test_target_postprocess` — основной сценарий (3 записи).
- `tests/test_postprocess_target.py::test_target_postprocess_missing_columns` — поведение при отсутствии колонок.
- `tests/test_postprocess_target.py::test_target_protein_classification_fallback` — fallback классификации по JSON.
- Мини-инпут: `tests/data/target.csv`.

## 13. JSON-артефакт (упрощённо)
```json
{
  "inputs": [
    {"name": "target", "path_key": "target_csv"}
  ],
  "stages": [
    {"id": "S3", "desc": "enrich names and synonyms", "code_refs": ["library/transforms/target.py:269-328"]},
    {"id": "S4", "desc": "normalize EC and cellularity", "code_refs": ["library/transforms/target.py:331-360"]},
    {"id": "S5", "desc": "infer protein classification", "code_refs": ["library/transforms/target.py:364-577"]}
  ],
  "columns": [
    {"output": "synonyms", "sources": ["protein_name_canonical", "pref_name", "protein_name_alt", "gene_symbol_list", "target_components"], "transform": "concat → clean_pipe"},
    {"output": "cellularity", "sources": ["lineage_superkingdom", "lineage_phylum", "lineage_class"], "transform": "taxonomy buckets"}
  ],
  "joins": [],
  "config_used": ["files.target_csv", "pipeline.target.output_columns", "pipeline.target.type_map"],
  "outputs": [
    {"path": "data/output/target_postprocessed.csv"}
  ]
}
```
