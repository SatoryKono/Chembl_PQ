// ===== Changelog =====
// - Added unified provider routing for file, HTTP, and SharePoint sources.
// - Implemented SharePoint parameter validation with friendly error messaging.
// - Introduced loader configuration with early column selection and culture-aware typing.
// - Restored cellularity enrichment logic and aligned organism lineage mapping.
// - Stabilized header promotion with sanitization and deterministic fallback naming.
// - Normalized reusable column lists to prevent duplicate-field selection errors in downstream queries.
// - Protected safe expansion helper from conflicting column names before nested joins are expanded.
// - Wrapped IUPHAR expansions with safe helper to prevent duplicate protein classification collisions.
// - Removed residual protein_classifications column before joining IUPHAR data to avoid duplicate-field expansion errors.


// - Resolved target module naming collision causing missing 'main' column errors in exports.
// - Reintroduced typed TargetHandlers export for compatibility with dependent queries.


let
  // ===== Parameters =====
  // Paths: relative file locations. Encodings: named code pages. Delimiters: reusable separators. PathEncodingKeys: map path keys to encoding keys. Defaults: fallback keys for loaders. Tables: logical dataset handles for inputs/outputs.
  Parameters = [
    BasePath = "E:\github\ChEMBL_dataAcquisition\",    // пример: "E:\github\ChEMBL_dataAcquisition\"
    SourceKind = "File",  // "File" | "SharePoint" | "Http"
    SharePointSiteUrl = "", // TODO: указать URL сайта SharePoint при SourceKind = "SharePoint"
    Defaults = [
      EncodingKey = "Utf8",
      DelimiterKey = "Csv",
      Culture = "en-US"
    ],
    Paths = [
      document_csv           = "data\input\full\document2.csv",
      activity_csv           = "data\input\full\activity2.csv",
      testitem_csv           = "data\input\full\testitem.csv",
      citation_fraction_csv  = "dictionary\_curation\citation_fraction.csv",

      document_csv_out       = "data\output\document\output.document_20250921.csv",
      testitem_csv_out       = "data\output\testitem\output.testitem_all.csv",
      assay_csv_out          = "data\output\assay\output.assay.csv",
      target_csv_out         = "data\output\targets\output.target.csv"
    ],
    Encodings = [
      Utf8 = 65001,
      Latin1 = 1252
    ],
    Delimiters = [
      Csv = ","
    ],
    PathEncodingKeys = [
      document_csv = "Utf8",
      activity_csv = "Utf8",
      testitem_csv = "Utf8",
      citation_fraction_csv = "Latin1",

      document_csv_out = "Latin1",
      testitem_csv_out = "Latin1",
      assay_csv_out = "Latin1",
      target_csv_out = "Latin1"
    ],
    Tables = [
      DocumentIn = "Document_in",
      ActivityIn = "Activity_in",
      TestitemIn = "Testitem_in",
      DocumentOut = "Document_out",
      TestitemOut = "Testitem_out",
      AssayOut = "Assay_out",
      TargetOut = "Target_out"
    ],
    TableLoadConfigs = [
      Document_in = [
        PathKey = "document_csv"
      ],
      Activity_in = [
        PathKey = "activity_csv",
        SelectColumns = {
          "activity_chembl_id",
          "assay_chembl_id",
          "molecule_chembl_id",
          "document_chembl_id",
          "is_citation"
        }
      ],
      Testitem_in = [
        PathKey = "testitem_csv"
      ],
      Target_out = [
        PathKey = "target_csv_out"
      ],
      Document_out = [
        PathKey = "document_csv_out"
      ],
      Assay_out = [
        PathKey = "assay_csv_out"
      ],
      Testitem_out = [
        PathKey = "testitem_csv_out"
      ],
      CitationFraction = [
        PathKey = "citation_fraction_csv",
        SelectColumns = {"N", "K_min_significant", "test_used_at_threshold", "p_value_at_threshold"},
        Schema = {
          {"N", Int64.Type},
          {"K_min_significant", Int64.Type},
          {"test_used_at_threshold", type text},
          {"p_value_at_threshold", type number}
        }
      ]
    ],
    TextCleanup = [
      TrimChars = {" ", Character.FromNumber(9), Character.FromNumber(10), Character.FromNumber(13)},
      PipeSeparator = "|",
      Space = " "
    ],
    DoiNormalization = [
      Prefixes = {"https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/", "doi:", "doi.org/"},
      TrimChars = {" ", ".", ";", ",", ":", ")", "]", "}", ">", Character.FromNumber(34), "'"},
      EncodedSeparators = {"%2f", "%2F"},
      Separator = "/",
      AuthorityPrefix = "10.",
      MinLength = 5,
      MaxLength = 300
    ]
  ],

  // ===== Providers =====
  BuildAbsolutePath = (pathRel as text) as text => Parameters[BasePath] & pathRel,
  FileProvider = (pathRel as text) as binary =>
    let
      absPath = BuildAbsolutePath(pathRel),
      // ===== External: try reading file system content =====
      contentProbe = try File.Contents(absPath)
    in
      if contentProbe[HasError] then
        error Error.Record(
          "FileProvider",
          "Не удалось прочитать файл. Проверьте путь и доступ к источнику.",
          [Path = absPath, Reason = contentProbe[Error]]
        )
      else
        contentProbe[Value],
  HttpProvider = (pathRel as text) as binary =>
    let
      absUrl = BuildAbsolutePath(pathRel),
      // ===== External: try fetching HTTP content =====
      contentProbe = try Web.Contents(absUrl)
    in
      if contentProbe[HasError] then
        error Error.Record(
          "HttpProvider",
          "Не удалось получить данные по HTTP. Проверьте URL и сетевой доступ.",
          [Url = absUrl, Reason = contentProbe[Error]]
        )
      else
        contentProbe[Value],
  SharePointProvider = (pathRel as text) as binary =>
    let
      siteUrlRaw = Parameters[SharePointSiteUrl],
      siteUrl =
        let
          trimmed = if siteUrlRaw = null then "" else Text.Trim(siteUrlRaw)
        in
          if trimmed = "" then
            error Error.Record(
              "SharePointProvider",
              "Параметр SharePointSiteUrl не задан. Укажите корректный URL сайта.",
              [SharePointSiteUrl = siteUrlRaw]
            )
          else
            trimmed,
      normalizedPath = Text.Replace(pathRel, "\\", "/"),
      pathSegments = List.Transform(Text.Split(normalizedPath, "/"), each Text.Trim(_)),
      segments =
        let
          filtered = List.Select(pathSegments, each _ <> "")
        in
          if List.IsEmpty(filtered) then
            error Error.Record(
              "SharePointProvider",
              "Путь к файлу на SharePoint не должен быть пустым.",
              [RelativePath = pathRel]
            )
          else
            filtered,
      // ===== External: try connecting to SharePoint site =====
      rootProbe = try SharePoint.Contents(siteUrl),
      rootTable =
        if rootProbe[HasError] then
          error Error.Record(
            "SharePointProvider",
            "Не удалось подключиться к SharePoint. Проверьте URL и учетные данные.",
            [SharePointSiteUrl = siteUrl, Reason = rootProbe[Error]]
          )
        else
          rootProbe[Value],
      navigate = (state as any, segment as text) as any =>
        let
          ensureTable =
            if Value.Is(state, type table) then
              state
            else
              error Error.Record(
                "SharePointProvider",
                "Неверная структура навигации SharePoint. Ожидалась папка.",
                [RelativePath = pathRel, Segment = segment]
              ),
          match = Table.SelectRows(ensureTable, each [Name] = segment),
          hasMatch = Table.RowCount(match) > 0
        in
          if not hasMatch then
            error Error.Record(
              "SharePointProvider",
              "Указанный путь не найден на SharePoint.",
              [RelativePath = pathRel, MissingSegment = segment]
            )
          else
            match{0}[Content],
      resolvedContent = List.Accumulate(segments, rootTable, navigate),
      binaryContent =
        if Value.Is(resolvedContent, type binary) then
          resolvedContent
        else if Value.Is(resolvedContent, type table) then
          error Error.Record(
            "SharePointProvider",
            "Путь указывает на папку SharePoint, а не на файл.",
            [RelativePath = pathRel]
          )
        else
          error Error.Record(
            "SharePointProvider",
            "Тип содержимого SharePoint не поддерживается.",
            [RelativePath = pathRel, ContentType = Value.Type(resolvedContent)]
          )
    in
      binaryContent,
  ProviderHandlers = [
    File = FileProvider,
    Http = HttpProvider,
    SharePoint = SharePointProvider
  ],
  Providers =
    Record.AddField(
      ProviderHandlers,
      "GetBinary",
      (sourceKind as text, identifier as text) as binary =>
        let
          handler = Record.FieldOrDefault(ProviderHandlers, sourceKind, null)
        in
          if handler = null then
            error Error.Record(
              "Providers",
              "Тип источника не поддерживается.",
              [SourceKind = sourceKind]
            )
          else
            handler(identifier)
    ),
  CheckPathExists = (pathRel as text) as logical =>
    let
      sourceKind = Parameters[SourceKind],
      exists =
        if sourceKind = "File" then
          let
            probe = try Providers[GetBinary](sourceKind, pathRel) otherwise null
          in
            probe <> null
        else
          true
    in
      exists,

  // ===== Loaders =====
  Loaders =
    let
      defaults = Parameters[Defaults],
      encodings = Parameters[Encodings],
      delimiters = Parameters[Delimiters],
      pathEncodingKeys = Parameters[PathEncodingKeys],
      tableConfigs = Record.FieldOrDefault(Parameters, "TableLoadConfigs", []),
      resolveEncoding = (pathKey as nullable text, options as nullable record) as number =>
        let
          opts = if options = null then [] else options,
          explicitEncoding = if Record.HasFields(opts, {"Encoding"}) then Record.Field(opts, "Encoding") else null,
          encodingKeyCandidate =
            if explicitEncoding <> null then
              null
            else if Record.HasFields(opts, {"EncodingKey"}) then
              let
                key = Record.Field(opts, "EncodingKey")
              in
                if key = null then null else key
            else if pathKey <> null then
              Record.FieldOrDefault(pathEncodingKeys, pathKey, defaults[EncodingKey])
            else
              defaults[EncodingKey],
          finalEncodingKey = if encodingKeyCandidate = null then defaults[EncodingKey] else encodingKeyCandidate,
          resolved = if explicitEncoding <> null then explicitEncoding else Record.Field(encodings, finalEncodingKey)
        in
          resolved,
      resolveDelimiter = (options as nullable record) as text =>
        let
          opts = if options = null then [] else options,
          delimiterKeyCandidate =
            if Record.HasFields(opts, {"DelimiterKey"}) then
              let
                key = Record.Field(opts, "DelimiterKey")
              in
                if key = null then null else key
            else
              defaults[DelimiterKey],
          finalKey = if delimiterKeyCandidate = null then defaults[DelimiterKey] else delimiterKeyCandidate
        in
          Record.Field(delimiters, finalKey),
      applySelectColumns = (tbl as table, options as nullable record) as table =>
        let
          opts = if options = null then [] else options,
          columnsRaw =
            if Record.HasFields(opts, {"SelectColumns"}) then
              Record.Field(opts, "SelectColumns")
            else
              null,
          columnsSanitized =
            if columnsRaw = null then
              {}
            else
              NormalizeColumnList(columnsRaw)
        in
          if columnsRaw = null then
            tbl
          else
            Table.SelectColumns(tbl, columnsSanitized, MissingField.Ignore),
      loadCsv = (pathRel as text, optional options as nullable record) as table =>
        let
          opts = if options = null then [] else options,
          pathKey = if Record.HasFields(opts, {"PathKey"}) then Record.Field(opts, "PathKey") else null,
          binary = Providers[GetBinary](Parameters[SourceKind], pathRel),
          csv = Csv.Document(
            binary,
            [
              Delimiter = resolveDelimiter(opts),
              Encoding = resolveEncoding(pathKey, opts)
            ]
          ),
          promoteHeaders = if Record.HasFields(opts, {"PromoteHeaders"}) then Record.Field(opts, "PromoteHeaders") else true,
          promoteContext = [
            SourceKind = Parameters[SourceKind],
            PathKey = pathKey,
            RelativePath = pathRel
          ],
          withHeaders = if promoteHeaders then PromoteHeadersSafe(csv, promoteContext) else csv,
          result = applySelectColumns(withHeaders, opts)
        in
          result,
      loadCsvWithSchema = (pathRel as text, schema as list, optional options as nullable record) as table =>
        let
          raw = loadCsv(pathRel, options),
          typed = TransformColumnTypesSafe(raw, schema)
        in
          typed,
      loadFromTableConfig = (configKey as text) as table =>
        let
          config = Record.FieldOrDefault(tableConfigs, configKey, null)
        in
          if config = null then
            error Error.Record(
              "Loaders.LoadFromTableConfig",
              "Конфигурация загрузки не найдена.",
              [ConfigKey = configKey]
            )
          else
            let
              pathKey = Record.Field(config, "PathKey"),
              relativePath = Record.Field(Parameters[Paths], pathKey),
              options = [
                PathKey = pathKey,
                EncodingKey = Record.FieldOrDefault(config, "EncodingKey", null),
                DelimiterKey = Record.FieldOrDefault(config, "DelimiterKey", null),
                SelectColumns = Record.FieldOrDefault(config, "SelectColumns", null),
                PromoteHeaders = Record.FieldOrDefault(config, "PromoteHeaders", true)
              ],
              schema = Record.FieldOrDefault(config, "Schema", null)
            in
              if schema <> null then
                loadCsvWithSchema(relativePath, schema, options)
              else
                loadCsv(relativePath, options)
    in
      [
        LoadCsv = loadCsv,
        LoadCsvWithSchema = loadCsvWithSchema,
        LoadFromTableConfig = loadFromTableConfig
      ],
  Data =
    let
      load = Loaders[LoadFromTableConfig]
    in
      [
        Document_in = load("Document_in"),
        Activity_in = load("Activity_in"),
        Testitem_in = load("Testitem_in"),

        Target_out = load("Target_out"),
        Document_out = load("Document_out"),
        Assay_out = load("Assay_out"),
        Testitem_out = load("Testitem_out")
      ],
  ParamsSummary =
    let
      pathKeys = Record.FieldNames(Parameters[Paths]),
      rows = List.Transform(
        pathKeys,
        (k as text) =>
          let
            rel = Record.Field(Parameters[Paths], k),
            encKey = Record.FieldOrDefault(Parameters[PathEncodingKeys], k, null),
            encValue = if encKey <> null then Record.FieldOrDefault(Parameters[Encodings], encKey, null) else null,
            available = CheckPathExists(rel)
          in
            [path_key = k, relative_path = rel, encoding_key = encKey, encoding_value = encValue, available = available]
      ),
      header = {"path_key", "relative_path", "encoding_key", "encoding_value", "available"},
      summary = #table(header, List.Transform(rows, each {_[path_key], _[relative_path], _[encoding_key], _[encoding_value], _[available]}))
    in
      summary,
// ===== Helpers =====
  Helpers =
    let
      textParams = Parameters[TextCleanup],
      doiParams = Parameters[DoiNormalization],
      toText = (value as any) as text => if value = null then "" else Text.From(value),
      normalizeWhitespace = (value as any, optional forceLower as nullable logical) as nullable text =>
        let
          shouldLower = if forceLower = null then true else forceLower,
          asText = if value = null then null else toText(value),
          cleaned = if asText = null then null else Text.Clean(asText),
          trimmed = if cleaned = null then null else Text.Trim(cleaned, textParams[TrimChars]),
          normalized =
            if trimmed = null or trimmed = "" then
              null
            else if shouldLower then
              Text.Lower(trimmed)
            else
              trimmed
        in
          normalized,
      cleanPipe = (txt as any, optional alias as nullable record, optional drop as nullable list, optional sort as nullable logical) as text =>
        let
          normalized = normalizeWhitespace(txt),
          parts =
            if normalized = null then
              {}
            else
              Text.Split(normalized, textParams[PipeSeparator]),
          trimmedParts = List.Select(List.Transform(parts, each normalizeWhitespace(_, true)), each _ <> null),
          mapped =
            if alias <> null then
              List.Transform(trimmedParts, each if Record.HasFields(alias, _) then Record.Field(alias, _) else _)
            else
              trimmedParts,
          filtered = if drop <> null then List.Select(mapped, each not List.Contains(drop, _)) else mapped,
          dedup = List.Distinct(filtered),
          ordered = if sort = true then List.Sort(dedup) else dedup,
          result = Text.Combine(ordered, textParams[PipeSeparator])
        in
          result,
      ensureColumn = (tbl as table, columnName as text, optional defaultValue as any, optional columnType as nullable type) as table =>
        let
          hasColumn = List.Contains(Table.ColumnNames(tbl), columnName),
          result =
            if hasColumn then
              tbl
            else if columnType <> null then
              Table.AddColumn(tbl, columnName, each defaultValue, columnType)
            else
              Table.AddColumn(tbl, columnName, each defaultValue)
        in
          result,
      zipLists = (firstList as list, secondList as list) as list =>
        let
          firstCount = List.Count(firstList),
          secondCount = List.Count(secondList),
          commonLength = List.Min({firstCount, secondCount}),
          truncatedFirst = List.FirstN(firstList, commonLength),
          truncatedSecond = List.FirstN(secondList, commonLength),
          positions = List.Positions(truncatedFirst),
          paired = List.Transform(positions, each {truncatedFirst{_}, truncatedSecond{_}})
        in
          paired,
      promoteHeadersSafe = (tbl as table, optional context as nullable record) as table =>
        let
          raiseError = (reason as any) =>
            let
              details =
                if context = null then
                  [Reason = reason]
                else
                  Record.Combine({context, [Reason = reason]})
            in
              error Error.Record(
                "Helpers.PromoteHeadersSafe",
                "Не удалось преобразовать первую строку в заголовки.",
                details
              ),
          rowCount = Table.RowCount(tbl),
          validatedTable = if rowCount = 0 then raiseError("TableIsEmpty") else tbl,
          columnNames = Table.ColumnNames(validatedTable),
          firstRow = Table.First(validatedTable),
          buildCandidate = (colName as text) as text =>
            let
              rawValue = Record.Field(firstRow, colName),
              normalizedAttempt = try normalizeWhitespace(rawValue, false) otherwise null,
              normalizedValue =
                if Value.Is(normalizedAttempt, type text) or normalizedAttempt = null then
                  normalizedAttempt
                else
                  null,
              fallbackName = colName
            in
              if normalizedValue = null then fallbackName else normalizedValue,
          headerCandidates = List.Transform(columnNames, each buildCandidate(_)),
          ensureUniqueNames = (names as list) as list =>
            let
              updateCounters = (counters as record, key as text, value as number) as record =>
                let
                  sanitized = if Record.HasFields(counters, key) then Record.RemoveFields(counters, {key}) else counters,
                  updated = Record.AddField(sanitized, key, value)
                in
                  updated,
              folder =
                List.Accumulate(
                  names,
                  [Result = {}, Counters = []],
                  (state as record, current as text) as record =>
                    let
                      counters = state[Counters],
                      currentCount = if Record.HasFields(counters, current) then Record.Field(counters, current) else 0,
                      nextIndex = currentCount + 1,
                      nextName = if currentCount = 0 then current else current & Text.From(nextIndex),
                      updatedCounters = updateCounters(counters, current, nextIndex),
                      updatedResult = state[Result] & {nextName}
                    in
                      [Result = updatedResult, Counters = updatedCounters]
                )
            in
              folder[Result],
          sanitizedHeaders = ensureUniqueNames(headerCandidates),
          renamePairs = zipLists(columnNames, sanitizedHeaders),
          body = Table.Skip(validatedTable, 1),
          renamed =
            if List.Count(renamePairs) = 0 then
              body
            else
              Table.RenameColumns(body, renamePairs, MissingField.Ignore)
        in
          renamed,
      transformColumnTypesSafe = (tbl as table, typeList as list, optional culture as nullable text) as table =>
        let
          columnNames = Table.ColumnNames(tbl),
          existing = List.Select(typeList, each List.Contains(columnNames, _{0})),
          defaultCulture = Parameters[Defaults][Culture],
          effectiveCulture = if culture <> null then culture else defaultCulture,
          transformed =
            if effectiveCulture = null then
              Table.TransformColumnTypes(tbl, existing)
            else
              Table.TransformColumnTypes(tbl, existing, effectiveCulture)
        in
          transformed,
      removeColumnsSafe = (tbl as table, columns as list) as table =>
        if columns = null or List.Count(columns) = 0 then
          tbl
        else
          Table.RemoveColumns(tbl, columns, MissingField.Ignore),
      dropColumns = (tbl as table, optional columns as nullable list) as table =>
        let
          cols = if columns = null then {} else columns
        in
          if List.Count(cols) = 0 then
            tbl
          else
            Table.RemoveColumns(tbl, cols, MissingField.Ignore),
      normalizeColumnList = (columns as nullable list) as list =>
        let
          base = if columns = null then {} else columns,
          nonNull = List.Select(base, each _ <> null),
          distinct = List.Distinct(nonNull)
        in
          distinct,
      selectColumnsInOrder = (tbl as table, optional columnOrder as nullable list) as table =>
        let
          order = normalizeColumnList(columnOrder),
          existing = List.Intersect({order, Table.ColumnNames(tbl)}),
          sanitized = normalizeColumnList(existing)
        in
          if List.Count(sanitized) = 0 then
            tbl
          else
            Table.SelectColumns(tbl, sanitized, MissingField.Ignore),
      renameColumnsSafe = (tbl as table, renames as list) as table =>
        if renames = null or List.Count(renames) = 0 then
          tbl
        else
          Table.RenameColumns(tbl, renames, MissingField.Ignore),
      replaceNullWithValue = (tbl as table, columns as list, replacement as any) as table =>
        if columns = null or List.Count(columns) = 0 then
          tbl
        else
          Table.ReplaceValue(tbl, null, replacement, Replacer.ReplaceValue, columns),
      lowercaseColumns = (tbl as table, columns as list) as table =>
        if columns = null or List.Count(columns) = 0 then
          tbl
        else
          Table.TransformColumns(tbl, List.Transform(columns, each {_, Text.Lower})),
      castToTextColumns = (tbl as table, columns as list, optional culture as nullable text) as table =>
        if columns = null or List.Count(columns) = 0 then
          tbl
        else
          transformColumnTypesSafe(tbl, List.Transform(columns, each {_, type text}), culture),
      expandTableColumnSafe = (
          tbl as table,
          columnName as text,
          columnsToExpand as list,
          optional newColumnNames as nullable list
        ) as table =>
        let
          columnExistsOriginal = List.Contains(Table.ColumnNames(tbl), columnName),
          sourcesRaw = if columnsToExpand = null then {} else columnsToExpand,
          targetsRaw = if newColumnNames = null then sourcesRaw else newColumnNames,
          rawPairs = zipLists(sourcesRaw, targetsRaw),
          filteredPairs = List.Select(rawPairs, each _{0} <> null and _{1} <> null),
          dedupPairs =
            List.Accumulate(
              filteredPairs,
              {},
              (state as list, pair as list) =>
                let
                  target = pair{1},
                  seenTargets = List.Transform(state, each _{1}),
                  exists = List.Contains(seenTargets, target)
                in
                  if exists then state else state & {pair}
            ),
          sourceColumns = List.Transform(dedupPairs, each _{0}),
          targetNames = List.Transform(dedupPairs, each _{1}),
          conflictTargets = List.RemoveItems(targetNames, {columnName}),
          conflicts =
            if columnExistsOriginal then
              List.Intersect({Table.ColumnNames(tbl), conflictTargets})
            else
              {},
          prepared =
            if List.IsEmpty(conflicts) then
              tbl
            else
              Table.RemoveColumns(tbl, conflicts, MissingField.Ignore),
          columnExists = List.Contains(Table.ColumnNames(prepared), columnName),
          expandAttempt =
            if not columnExists then
              [HasError = true, Error = "ColumnMissing"]
            else if List.IsEmpty(sourceColumns) then
              try Table.RemoveColumns(prepared, {columnName}, MissingField.Ignore)
            else
              try Table.ExpandTableColumn(prepared, columnName, sourceColumns, targetNames),
          fallbackExpand = () as table =>
            let
              nestedValues = if columnExists then Table.Column(prepared, columnName) else {},
              nestedTables = List.Select(nestedValues, each _ <> null and Value.Is(_, type table)),
              nestedColumns =
                if List.IsEmpty(nestedTables) then
                  {}
                else
                  List.Distinct(List.Combine(List.Transform(nestedTables, each Table.ColumnNames(_)))),
              validPairs = List.Select(dedupPairs, each List.Contains(nestedColumns, _{0})),
              validSources = List.Transform(validPairs, each _{0}),
              validTargets = List.Transform(validPairs, each _{1}),
              expanded =
                if List.IsEmpty(validSources) then
                  if columnExists then
                    Table.RemoveColumns(prepared, {columnName}, MissingField.Ignore)
                  else
                    prepared
                else
                  Table.ExpandTableColumn(prepared, columnName, validSources, validTargets),
              missingTargets = List.Transform(List.RemoveMatchingItems(dedupPairs, validPairs), each _{1}),
              completed =
                List.Accumulate(
                  missingTargets,
                  expanded,
                  (state as table, missingName as text) => ensureColumn(state, missingName, null, type any)
                )
            in
              completed
        in
          if not columnExistsOriginal then
            tbl
          else if expandAttempt[HasError] then
            fallbackExpand()
          else
            expandAttempt[Value],
      joinAndExpand = (
          source as table,
          sourceKeys as list,
          reference as table,
          referenceKeys as list,
          nestedColumnName as text,
          optional columnsToExpand as nullable list,
          optional newColumnNames as nullable list
        ) as table =>
        let
          joined = Table.NestedJoin(source, sourceKeys, reference, referenceKeys, nestedColumnName, JoinKind.LeftOuter),
          expanded =
            if columnsToExpand = null then
              joined
            else
              expandTableColumnSafe(joined, nestedColumnName, columnsToExpand, newColumnNames)
        in
          expanded,
      splitPipeList = (value as any, optional delimiter as nullable text) as list =>
        let
          sep = if delimiter = null then textParams[PipeSeparator] else delimiter,
          raw = toText(value),
          segments = if raw = "" then {} else Text.Split(raw, sep),
          trimmed = List.Transform(segments, each Text.Trim(_)),
          filtered = List.Select(trimmed, each _ <> "")
        in
          filtered,
      replaceTextInColumn = (tbl as table, columnName as text, replacements as list) as table =>
        if not List.Contains(Table.ColumnNames(tbl), columnName) then
          tbl
        else
          List.Accumulate(
            replacements,
            tbl,
            (state as table, replacement as record) =>
              let
                oldValue = Record.FieldOrDefault(replacement, "Old", null),
                newValue = Record.FieldOrDefault(replacement, "New", null)
              in
                if oldValue = null then
                  state
                else
                  Table.ReplaceValue(state, oldValue, newValue, Replacer.ReplaceText, {columnName})
          ),
      normalizePages = (value as any) as nullable text =>
        let
          base = normalizeWhitespace(value),
          replaced = if base = null then null else Text.Replace(Text.Replace(base, "–", "-"), "—", "-")
        in
          replaced,
      tryNumber = (value as any) as nullable number =>
        let
          direct = try Number.From(value) otherwise null,
          asText = if direct <> null then null else normalizeWhitespace(value, false),
          fromText = if asText = null then null else try Number.FromText(asText) otherwise null
        in
          if direct <> null then direct else fromText,
      normalizeDoi = (value as any) as nullable text =>
        let
          normalized = normalizeWhitespace(value),
          withoutPrefixes =
            if normalized = null then
              null
            else
              List.Accumulate(
                doiParams[Prefixes],
                normalized,
                (state, pref) => if state <> null and Text.StartsWith(state, pref) then Text.Range(state, Text.Length(pref)) else state
              ),
          decoded =
            if withoutPrefixes = null then
              null
            else
              List.Accumulate(
                doiParams[EncodedSeparators],
                withoutPrefixes,
                (state, code) => if state = null then null else Text.Replace(state, code, doiParams[Separator])
              ),
          trimmed = if decoded = null then null else Text.Trim(decoded, doiParams[TrimChars]),
          compact = if trimmed = null then null else Text.Replace(trimmed, textParams[Space], "")
        in
          if compact = null or compact = "" then null else compact
    in
      [
        ToText = toText,
        NormalizeWhitespace = normalizeWhitespace,
        CleanPipe = cleanPipe,
        EnsureColumn = ensureColumn,
        PromoteHeadersSafe = promoteHeadersSafe,
        TransformColumnTypesSafe = transformColumnTypesSafe,
        NormalizePages = normalizePages,
        TryNumber = tryNumber,
        NormalizeDoi = normalizeDoi,
        RemoveColumnsSafe = removeColumnsSafe,
        DropColumns = dropColumns,
        NormalizeColumnList = normalizeColumnList,
        RenameColumnsSafe = renameColumnsSafe,
        ReplaceNullWithValue = replaceNullWithValue,
        LowercaseColumns = lowercaseColumns,
        CastToTextColumns = castToTextColumns,
        ExpandTableColumnSafe = expandTableColumnSafe,
        JoinAndExpand = joinAndExpand,
        SelectColumnsInOrder = selectColumnsInOrder,
        SplitPipeList = splitPipeList,
        ReplaceTextInColumn = replaceTextInColumn,
        ZipLists = zipLists
      ],
  ToText = Helpers[ToText],
  NormalizeWhitespace = Helpers[NormalizeWhitespace],
  CleanPipe = Helpers[CleanPipe],
  EnsureColumn = Helpers[EnsureColumn],
  PromoteHeadersSafe = Helpers[PromoteHeadersSafe],
  TransformColumnTypesSafe = Helpers[TransformColumnTypesSafe],
  NormalizePages = Helpers[NormalizePages],
  TryNumber = Helpers[TryNumber],
  NormalizeDoi = Helpers[NormalizeDoi],
  RemoveColumnsSafe = Helpers[RemoveColumnsSafe],
  DropColumns = Helpers[DropColumns],
  NormalizeColumnList = Helpers[NormalizeColumnList],
  RenameColumnsSafe = Helpers[RenameColumnsSafe],
  ReplaceNullWithValue = Helpers[ReplaceNullWithValue],
  LowercaseColumns = Helpers[LowercaseColumns],
  CastToTextColumns = Helpers[CastToTextColumns],
  ExpandTableColumnSafe = Helpers[ExpandTableColumnSafe],
  JoinAndExpand = Helpers[JoinAndExpand],
  SelectColumnsInOrder = Helpers[SelectColumnsInOrder],
  SplitPipeList = Helpers[SplitPipeList],
  ReplaceTextInColumn = Helpers[ReplaceTextInColumn],
  ZipLists = Helpers[ZipLists],
  SelectColumnsSafe = (tbl as table, columns as list) as table =>
    Table.SelectColumns(tbl, NormalizeColumnList(columns), MissingField.Ignore),
  FinalizeAggColumns = (tbl as table, aggColumns as list) as table =>
    let
      typePairs = List.Transform(aggColumns, each {_, Int64.Type}),
      typed = TransformColumnTypesSafe(tbl, typePairs),
      filled = Table.ReplaceValue(typed, null, 0, Replacer.ReplaceValue, aggColumns)
    in
      filled,
  BuildActivityAgg = (src as table) as table =>
    let
      prepared = EnsureColumn(src, "document_chembl_id", null, type text),
      selected = SelectColumnsSafe(prepared, {"document_chembl_id"}),
      grouped = Table.Group(selected, {"document_chembl_id"}, {{"n_activity", each Table.RowCount(_), Int64.Type}}),
      typed = TransformColumnTypesSafe(grouped, {{"document_chembl_id", type text}}),
      final = FinalizeAggColumns(typed, {"n_activity"})
    in
      final,
  BuildAssayAgg = (src as table) as table =>
    let
      colNames = Table.ColumnNames(src),
      hasDoc = List.Contains(colNames, "document_chembl_id"),
      hasAssay = List.Contains(colNames, "assay_chembl_id"),
      prepared = if hasDoc then EnsureColumn(src, "document_chembl_id", null, type text) else src,
      base =
        if hasDoc and hasAssay then
          let
            normalized = EnsureColumn(prepared, "assay_chembl_id", null, type text),
            pairs = SelectColumnsSafe(normalized, {"document_chembl_id", "assay_chembl_id"}),
            distinctPairs = Table.Distinct(pairs),
            grouped = Table.Group(distinctPairs, {"document_chembl_id"}, {{"n_assay", each Table.RowCount(_), Int64.Type}})
          in
            grouped
        else if hasDoc then
          let
            docs = Table.Distinct(SelectColumnsSafe(prepared, {"document_chembl_id"})),
            withZeros = Table.AddColumn(docs, "n_assay", each 0, Int64.Type)
          in
            withZeros
        else
          #table({"document_chembl_id", "n_assay"}, {}),
      typed = TransformColumnTypesSafe(base, {{"document_chembl_id", type text}}),
      final = FinalizeAggColumns(typed, {"n_assay"})
    in
      final,
  BuildTestitemAgg = (src as table) as table =>
    let
      withDoc = EnsureColumn(src, "document_chembl_id", null, type text),
      prepared = EnsureColumn(withDoc, "molecule_chembl_id", null, type text),
      docMol = SelectColumnsSafe(prepared, {"document_chembl_id", "molecule_chembl_id"}),
      distinct = Table.Distinct(docMol, {"document_chembl_id", "molecule_chembl_id"}),
      grouped = Table.Group(distinct, {"document_chembl_id"}, {{"n_testitem", each Table.RowCount(_), Int64.Type}}),
      typed = TransformColumnTypesSafe(grouped, {{"document_chembl_id", type text}}),
      final = FinalizeAggColumns(typed, {"n_testitem"})
    in
      final,
  BuildCitationAgg = (src as table) as table =>
    let
      withDoc = EnsureColumn(src, "document_chembl_id", null, type text),
      prepared = EnsureColumn(withDoc, "is_citation", false, type logical),
      docFlag = SelectColumnsSafe(prepared, {"document_chembl_id", "is_citation"}),
      filtered = Table.SelectRows(docFlag, each [is_citation] = true),
      grouped = Table.Group(filtered, {"document_chembl_id"}, {{"citations", each Table.RowCount(_), Int64.Type}}),
      typed = TransformColumnTypesSafe(grouped, {{"document_chembl_id", type text}}),
      final = FinalizeAggColumns(typed, {"citations"})
    in
      final,
// ===================== DocumentInput =====================
  ReferenceThresholdsRaw = Loaders[LoadFromTableConfig]("CitationFraction"),
  ReferenceThresholds = Table.Buffer(SelectColumnsSafe(ReferenceThresholdsRaw, {"N", "K_min_significant"})),
  ActivityRaw0 = Data[Activity_in],
  ActivityRaw = TransformColumnTypesSafe(
    ActivityRaw0,
    {
      {"activity_chembl_id", Int64.Type},
      {"assay_chembl_id", type text},
      {"molecule_chembl_id", type text},
      {"document_chembl_id", type text},
      {"is_citation", type logical}
    },
    null
  ),
  ActivityPrepared = EnsureColumn(ActivityRaw, "document_chembl_id", null, type text),
  ActivityBuffered = Table.Buffer(ActivityPrepared),
  ActivityAggregates = [
    activity = Table.Buffer(BuildActivityAgg(ActivityBuffered)),
    assay = Table.Buffer(BuildAssayAgg(ActivityBuffered)),
    testitem = Table.Buffer(BuildTestitemAgg(ActivityBuffered)),
    citation = Table.Buffer(BuildCitationAgg(ActivityBuffered))
  ],
  citations = [
    get_reference = () => Data[Document_in],
    get_activity_agg = () => ActivityAggregates[activity],
    get_assay_agg = () => ActivityAggregates[assay],
    get_testitem_agg = () => ActivityAggregates[testitem],
    get_citation_agg = () => ActivityAggregates[citation],
    get_citations_fraction = () =>
      let

        getReferenceThresholds = () as table =>
          let
            t0 = Loaders[LoadFromTableConfig]("CitationFraction"),
            t1 = SelectColumnsSafe(t0, {"N", "K_min_significant"})
          in
            t1,
        getActivityAgg = (src as table) as table =>
          let
            t = Table.Group(SelectColumnsSafe(src, {"document_chembl_id"}), {"document_chembl_id"}, {{"n_activity", each Table.RowCount(_), Int64.Type}})
          in
            t,
        getAssayAgg = (src as table) as table =>
          let
            colNames = Table.ColumnNames(src),
            hasDoc = List.Contains(colNames, "document_chembl_id"),
            hasAssay = List.Contains(colNames, "assay_chembl_id"),
            result =
              if hasDoc and hasAssay then
                let
                  t0 = Table.Distinct(
                    SelectColumnsSafe(src, {"document_chembl_id", "assay_chembl_id"})
                  ),
                  t1 = Table.Group(t0, {"document_chembl_id"}, {{"n_assay", each Table.RowCount(_), Int64.Type}})
                in
                  t1
              else if hasDoc then
                let
                  docs = Table.Distinct(SelectColumnsSafe(src, {"document_chembl_id"}), {"document_chembl_id"}),
                  withZeros = Table.AddColumn(docs, "n_assay", each 0, Int64.Type)
                in
                  withZeros
              else
                TransformColumnTypesSafe(#table({"document_chembl_id", "n_assay"}, {}), {{"n_assay", Int64.Type}}),
            typed = TransformColumnTypesSafe(result, {{"n_assay", Int64.Type}}, "en-US")
          in
            typed,
        getTestItemAgg = (src as table) as table =>
          let
            t0 = Table.Distinct(SelectColumnsSafe(src, {"document_chembl_id", "molecule_chembl_id"}), {"molecule_chembl_id"}),
            t1 = Table.Group(t0, {"document_chembl_id"}, {{"n_testitem", each Table.RowCount(_), Int64.Type}})
          in
            t1,
        getCitationsAgg = (src as table) as table =>
          let
            t0 = Table.SelectRows(SelectColumnsSafe(src, {"document_chembl_id", "is_citation"}), each [is_citation] = true),
            t1 = Table.Group(t0, {"document_chembl_id"}, {{"citations", each Table.RowCount(_), Int64.Type}})
          in
            t1,
        ActivityRaw0 = Data[Activity_in],
        ActivityRaw = TransformColumnTypesSafe(
          ActivityRaw0,
          {
            {"activity_chembl_id", Int64.Type},
            {"assay_chembl_id", type text},
            {"molecule_chembl_id", type text},
            {"document_chembl_id", type text},
            {"is_citation", type logical}
          },
          null
        ),
        ActivityPrepared = EnsureColumn(ActivityRaw, "document_chembl_id", null, type text),
        AggActivity = getActivityAgg(ActivityPrepared),
        AggAssay = getAssayAgg(ActivityPrepared),
        AggTest = getTestItemAgg(ActivityPrepared),
        AggCit = getCitationsAgg(ActivityPrepared),

        RA = Table.RenameColumns(AggActivity, {{"document_chembl_id", "doc_id"}}),
        J1 = Table.Join(AggCit, "document_chembl_id", RA, "doc_id", JoinKind.LeftOuter),
        J1c = Table.RemoveColumns(J1, {"doc_id"}, MissingField.Ignore),
        RS = Table.RenameColumns(AggAssay, {{"document_chembl_id", "doc_id"}}),
        J2 = Table.Join(J1c, "document_chembl_id", RS, "doc_id", JoinKind.LeftOuter),
        J2c = Table.RemoveColumns(J2, {"doc_id"}, MissingField.Ignore),
        RT = Table.RenameColumns(AggTest, {{"document_chembl_id", "doc_id"}}),
        J3 = Table.Join(J2c, "document_chembl_id", RT, "doc_id", JoinKind.LeftOuter),
        Aggregated = FinalizeAggColumns(J3, {"n_activity", "citations", "n_assay", "n_testitem"}),
        RefR = Table.RenameColumns(ReferenceThresholds, {{"N", "N_key"}}),
        J4 = Table.Join(Aggregated, "n_activity", RefR, "N_key", JoinKind.LeftOuter),
        Result = Table.AddColumn(
          J4,
          "significant_citations_fraction",
          each try [citations] > [K_min_significant] otherwise false,
          type logical
        ),
        Final = Table.ReorderColumns(
          Result,
          {"document_chembl_id", "n_activity", "citations", "n_assay", "n_testitem", "K_min_significant", "significant_citations_fraction"},
          MissingField.Ignore
        )
      in
        Final
  ],
  DocumentInput =
    let
      fromPubMed = (source as table) as table =>
        let
          columnsToRemove = {
            "PubMed.JournalTitle",
            "scholar.PMID",
            "scholar.DOI",
            "scholar.PublicationTypes",
            "scholar.Venue",
            "scholar.SemanticScholarId",
            "scholar.ExternalIds",
            "scholar.Error",
            "OpenAlex.PMID",
            "OpenAlex.DOI",
            "OpenAlex.PublicationTypes",
            "OpenAlex.TypeCrossref",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeshDescriptors",
            "OpenAlex.MeshQualifiers",
            "OpenAlex.Id",
            "OpenAlex.Error",
            "crossref.DOI",
            "crossref.Type",
            "crossref.Subtype",
            "crossref.Title",
            "crossref.Subtitle",
            "crossref.Subject",
            "crossref.Error",
            "publication_types_normalised",
            "publication_review_score",
            "publication_experimental_score",
            "publication_class",
            "ChEMBL.title",
            "ChEMBL.abstract",
            "ChEMBL.doi",
            "ChEMBL.year",
            "ChEMBL.journal",
            "ChEMBL.journal_abbrev",
            "ChEMBL.volume",
            "ChEMBL.issue",
            "ChEMBL.first_page",
            "ChEMBL.last_page",
            "ChEMBL.pubmed_id",
            "ChEMBL.authors",
            "ChEMBL.source"
          },
          dropNoise = RemoveColumnsSafe(source, columnsToRemove),
          numericToFill = {"PubMed.Volume", "PubMed.Issue", "PubMed.StartPage", "PubMed.EndPage"},
          filledNumeric = ReplaceNullWithValue(dropNoise, numericToFill, 0),
          textColumns = {"PubMed.ArticleTitle", "PubMed.Abstract"},
          textPrepared = CastToTextColumns(filledNumeric, textColumns),
          dateColumns = {
            "PubMed.YearCompleted",
            "PubMed.MonthCompleted",
            "PubMed.DayCompleted",
            "PubMed.YearRevised",
            "PubMed.MonthRevised",
            "PubMed.DayRevised"
          },
          datesAsText = CastToTextColumns(textPrepared, dateColumns, "en-US"),
          lowerTargets = {"PubMed.PublicationType", "PubMed.MeSH_Descriptors", "PubMed.MeSH_Qualifiers", "PubMed.ChemicalList"} & dateColumns,
          lowered = LowercaseColumns(datesAsText, lowerTargets),
          filledDates = ReplaceNullWithValue(lowered, dateColumns, "0"),
          pageColumns = {"PubMed.Volume", "PubMed.Issue", "PubMed.StartPage", "PubMed.EndPage"},
          pagesAsText = CastToTextColumns(filledDates, pageColumns),
          renamedVolumeIssue = RenameColumnsSafe(pagesAsText, {{"PubMed.Volume", "volume"}, {"PubMed.Issue", "issue"}}),
          combinedPages = Table.CombineColumns(renamedVolumeIssue, {"PubMed.StartPage", "PubMed.EndPage"}, Combiner.CombineTextByDelimiter("-", QuoteStyle.None), "pages"),
          renameMap = {
            {"PubMed.ISSN", "ISSN"},
            {"PubMed.PublicationType", "publication_type"},
            {"PubMed.PMID", "PMID"},
            {"PubMed.DOI", "DOI"},
            {"PubMed.ArticleTitle", "title"},
            {"PubMed.Abstract", "abstract"},
            {"PubMed.JournalISOAbbrev", ".journal"},
            {"PubMed.YearCompleted", "completed.year"},
            {"PubMed.MonthCompleted", "completed.month"},
            {"PubMed.DayCompleted", "completed.day"},
            {"PubMed.YearRevised", "revised.year"},
            {"PubMed.MonthRevised", "revised.month"},
            {"PubMed.DayRevised", "revised.day"},
            {"PubMed.Error", "error"},
            {"PubMed.MeSH_Descriptors", "MeSH.descriptors"}
          },
          renamed = RenameColumnsSafe(combinedPages, renameMap),
          ordered = Table.ReorderColumns(
            renamed,
            {
              "PMID",
              "DOI",
              "title",
              "abstract",
              "ISSN",
              ".journal",
              "volume",
              "issue",
              "pages",
              "publication_type",
              "MeSH.descriptors",
              "PubMed.MeSH_Qualifiers",
              "PubMed.ChemicalList",
              "completed.year",
              "completed.month",
              "completed.day",
              "revised.year",
              "revised.month",
              "revised.day",
              "error"
            },
            MissingField.Ignore
          ),
          loweredFinal = LowercaseColumns(ordered, {"publication_type", "title", "abstract", "DOI", ".journal"}),
          final = RenameColumnsSafe(loweredFinal, {{"pages", "page"}, {"DOI", "PubMed.doi"}})
        in
          final,
        fromScholar = (source as table) as table =>
          let
          columnsToRemove = {
            "PubMed.PMID",
            "PubMed.DOI",
            "PubMed.ArticleTitle",
            "PubMed.Abstract",
            "PubMed.JournalTitle",
            "PubMed.JournalISOAbbrev",
            "PubMed.Volume",
            "PubMed.Issue",
            "PubMed.StartPage",
            "PubMed.EndPage",
            "PubMed.ISSN",
            "PubMed.PublicationType",
            "PubMed.MeSH_Descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "PubMed.YearCompleted",
            "PubMed.MonthCompleted",
            "PubMed.DayCompleted",
            "PubMed.YearRevised",
            "PubMed.MonthRevised",
            "PubMed.DayRevised",
            "PubMed.Error",
            "OpenAlex.PMID",
            "OpenAlex.DOI",
            "OpenAlex.PublicationTypes",
            "OpenAlex.TypeCrossref",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeshDescriptors",
            "OpenAlex.MeshQualifiers",
            "OpenAlex.Id",
            "OpenAlex.Error",
            "crossref.DOI",
            "crossref.Type",
            "crossref.Subtype",
            "crossref.Title",
            "crossref.Subtitle",
            "crossref.Subject",
            "crossref.Error",
            "publication_types_normalised",
            "publication_review_score",
            "publication_experimental_score",
            "publication_class",
            "ChEMBL.document_chembl_id",
            "ChEMBL.title",
            "ChEMBL.abstract",
            "ChEMBL.doi",
            "ChEMBL.year",
            "ChEMBL.journal",
            "ChEMBL.journal_abbrev",
            "ChEMBL.volume",
            "ChEMBL.issue",
            "ChEMBL.first_page",
            "ChEMBL.last_page",
            "ChEMBL.pubmed_id",
            "ChEMBL.authors",
            "ChEMBL.source"
          },
          dropNoise = RemoveColumnsSafe(source, columnsToRemove),
          textColumns = {
            "scholar.ExternalIds",
            "scholar.PublicationTypes",
            "scholar.Venue",
            "scholar.DOI"
          },
          typed = CastToTextColumns(dropNoise, textColumns),
          zeroToNull = Table.ReplaceValue(typed, "0", null, Replacer.ReplaceValue, {"scholar.DOI", "scholar.PublicationTypes"}),
          lowered = LowercaseColumns(zeroToNull, {"scholar.DOI", "scholar.PublicationTypes", "scholar.Venue", "scholar.ExternalIds"}),
          fixedError = Table.ReplaceValue(lowered, 0, null, Replacer.ReplaceValue, {"scholar.Error"}),
          final = RenameColumnsSafe(fixedError, {{"scholar.DOI", "scholar.doi"}})
        in
          final,
        fromChEMBL = (source as table) as table =>
          let
          columnsToRemove = {
            "PubMed.ArticleTitle",
            "PubMed.Abstract",
            "PubMed.JournalTitle",
            "PubMed.JournalISOAbbrev",
            "PubMed.Volume",
            "PubMed.Issue",
            "PubMed.StartPage",
            "PubMed.EndPage",
            "PubMed.ISSN",
            "PubMed.PublicationType",
            "PubMed.MeSH_Descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "PubMed.YearCompleted",
            "PubMed.MonthCompleted",
            "PubMed.DayCompleted",
            "PubMed.YearRevised",
            "PubMed.MonthRevised",
            "PubMed.DayRevised",
            "PubMed.Error",
            "scholar.PMID",
            "scholar.DOI",
            "scholar.PublicationTypes",
            "scholar.Venue",
            "scholar.SemanticScholarId",
            "scholar.ExternalIds",
            "scholar.Error",
            "OpenAlex.PMID",
            "OpenAlex.DOI",
            "OpenAlex.PublicationTypes",
            "OpenAlex.TypeCrossref",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeshDescriptors",
            "OpenAlex.MeshQualifiers",
            "OpenAlex.Id",
            "OpenAlex.Error",
            "crossref.DOI",
            "crossref.Type",
            "crossref.Subtype",
            "crossref.Title",
            "crossref.Subtitle",
            "crossref.Subject",
            "crossref.Error",
            "publication_types_normalised",
            "publication_review_score",
            "publication_experimental_score",
            "publication_class",
            "ChEMBL.pubmed_id",
            "ChEMBL.source",
            "ChEMBL.journal"
          },
          dropNoise = RemoveColumnsSafe(source, columnsToRemove),
          typed = CastToTextColumns(dropNoise, {"ChEMBL.volume", "ChEMBL.issue", "ChEMBL.doi"}),
          renamedIds = RenameColumnsSafe(typed, {{"PubMed.PMID", "PMID"}, {"PubMed.DOI", "DOI"}}),
          renamedCore = RenameColumnsSafe(renamedIds, {{"ChEMBL.title", "title"}, {"ChEMBL.document_chembl_id", "document_chembl_id"}, {"ChEMBL.abstract", "abstract"}}),
          loweredDoi = LowercaseColumns(renamedCore, {"DOI", "ChEMBL.doi"}),
          withSameDoi = Table.AddColumn(loweredDoi, "same_doi", each [DOI] = [ChEMBL.doi], type logical),
          renamedMeta = RenameColumnsSafe(
            withSameDoi,
            {
              {"ChEMBL.journal_abbrev", "journal"},
              {"ChEMBL.volume", "volume"},
              {"ChEMBL.issue", "issue"},
              {"ChEMBL.authors", "authors"},
              {"ChEMBL.year", "year"}
            }
          ),
          filledNulls = ReplaceNullWithValue(renamedMeta, {"volume", "issue", "ChEMBL.first_page", "ChEMBL.last_page"}, 0),
          loweredText = LowercaseColumns(filledNulls, {"title", "abstract", "ChEMBL.doi", "authors", "journal"}),
          pagesAsText = CastToTextColumns(loweredText, {"ChEMBL.first_page", "ChEMBL.last_page"}, "en-US"),
          combinedPages = Table.CombineColumns(pagesAsText, {"ChEMBL.first_page", "ChEMBL.last_page"}, Combiner.CombineTextByDelimiter("-", QuoteStyle.None), "page"),
          final = RemoveColumnsSafe(combinedPages, {"DOI"})
        in
          final,
        fromOpenAlex = (source as table) as table =>
          let
          typed = TransformColumnTypesSafe(source, {{"PubMed.PMID", Int64.Type}, {"PubMed.DOI", type text}, {"OpenAlex.DOI", type text}}),
          columnsToRemove = {
            "PubMed.ArticleTitle",
            "PubMed.Abstract",
            "PubMed.JournalTitle",
            "PubMed.JournalISOAbbrev",
            "PubMed.Volume",
            "PubMed.Issue",
            "PubMed.StartPage",
            "PubMed.EndPage",
            "PubMed.ISSN",
            "PubMed.PublicationType",
            "PubMed.MeSH_Descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "PubMed.YearCompleted",
            "PubMed.MonthCompleted",
            "PubMed.DayCompleted",
            "PubMed.YearRevised",
            "PubMed.MonthRevised",
            "PubMed.DayRevised",
            "PubMed.Error",
            "scholar.PMID",
            "scholar.DOI",
            "scholar.PublicationTypes",
            "scholar.Venue",
            "scholar.SemanticScholarId",
            "scholar.ExternalIds",
            "scholar.Error",
            "ChEMBL.document_chembl_id",
            "ChEMBL.title",
            "ChEMBL.abstract",
            "ChEMBL.doi",
            "ChEMBL.year",
            "ChEMBL.journal",
            "ChEMBL.journal_abbrev",
            "ChEMBL.volume",
            "ChEMBL.issue",
            "ChEMBL.first_page",
            "ChEMBL.last_page",
            "ChEMBL.pubmed_id",
            "ChEMBL.authors",
            "ChEMBL.source",
            "publication_types_normalised",
            "publication_review_score",
            "publication_experimental_score",
            "publication_class",
            "crossref.DOI",
            "crossref.Type",
            "crossref.Subtype",
            "crossref.Title",
            "crossref.Subtitle",
            "crossref.Subject",
            "crossref.Error"
          },
          dropNoise = RemoveColumnsSafe(typed, columnsToRemove),
          renamedIds = RenameColumnsSafe(dropNoise, {{"PubMed.PMID", "PMID"}, {"PubMed.DOI", "DOI"}}),
          withSameDoi = Table.AddColumn(renamedIds, "same_doi", each [DOI] = [OpenAlex.DOI], type logical),
          renamedMeta = RenameColumnsSafe(
            withSameDoi,
            {
              {"OpenAlex.PublicationTypes", "publication_type"},
              {"OpenAlex.TypeCrossref", "crossref_type"},
              {"OpenAlex.MeshDescriptors", "MeSH.descriptors"}
            }
          ),
          dropQualifiers = RemoveColumnsSafe(renamedMeta, {"OpenAlex.MeshQualifiers"}),
          renamedIds2 = RenameColumnsSafe(dropQualifiers, {{"OpenAlex.Id", "id"}, {"OpenAlex.Error", "Error"}}),
          lowered = LowercaseColumns(renamedIds2, {"MeSH.descriptors", "crossref_type", "OpenAlex.DOI", "DOI"}),
          removedDoi = RemoveColumnsSafe(lowered, {"DOI"}),
          final = RenameColumnsSafe(removedDoi, {{"OpenAlex.DOI", "OpenAlex.doi"}})
        in
          final,
        fromCrossRef = (source as table) as table =>
          let
          typed = TransformColumnTypesSafe(
            source,
            {
              {"PubMed.PMID", Int64.Type},
              {"PubMed.DOI", type text},
              {"crossref.DOI", type text},
              {"crossref.Title", type text},
              {"crossref.Type", type text}
            }
          ),
          renamedIds = RenameColumnsSafe(typed, {{"PubMed.PMID", "PMID"}, {"PubMed.DOI", "DOI"}}),
          columnsToRemove = {
            "PubMed.ArticleTitle",
            "PubMed.Abstract",
            "PubMed.JournalTitle",
            "PubMed.JournalISOAbbrev",
            "PubMed.Volume",
            "PubMed.Issue",
            "PubMed.StartPage",
            "PubMed.EndPage",
            "PubMed.ISSN",
            "PubMed.PublicationType",
            "PubMed.MeSH_Descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "PubMed.YearCompleted",
            "PubMed.MonthCompleted",
            "PubMed.DayCompleted",
            "PubMed.YearRevised",
            "PubMed.MonthRevised",
            "PubMed.DayRevised",
            "PubMed.Error",
            "scholar.PMID",
            "scholar.DOI",
            "scholar.PublicationTypes",
            "scholar.Venue",
            "scholar.SemanticScholarId",
            "scholar.ExternalIds",
            "scholar.Error",
            "publication_types_normalised",
            "publication_review_score",
            "publication_experimental_score",
            "publication_class",
            "ChEMBL.document_chembl_id",
            "ChEMBL.title",
            "ChEMBL.abstract",
            "ChEMBL.doi",
            "ChEMBL.year",
            "ChEMBL.journal",
            "ChEMBL.journal_abbrev",
            "ChEMBL.volume",
            "ChEMBL.issue",
            "ChEMBL.first_page",
            "ChEMBL.last_page",
            "ChEMBL.pubmed_id",
            "ChEMBL.authors",
            "ChEMBL.source",
            "OpenAlex.PMID",
            "OpenAlex.DOI",
            "OpenAlex.PublicationTypes",
            "OpenAlex.TypeCrossref",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeshDescriptors",
            "OpenAlex.MeshQualifiers",
            "OpenAlex.Id",
            "OpenAlex.Error"
          },
          dropNoise = RemoveColumnsSafe(renamedIds, columnsToRemove),
          renamedMeta = RenameColumnsSafe(dropNoise, {{"crossref.Type", "publication_type"}, {"crossref.Title", "title"}, {"crossref.Error", "Error"}}),
          lowered = LowercaseColumns(renamedMeta, {"title"}),
          removedDoi = RemoveColumnsSafe(lowered, {"DOI"}),
          final = RenameColumnsSafe(removedDoi, {{"crossref.DOI", "crossref.doi"}})
        in
          final,
        mergeSources = (source as table) as table =>
          let
          pubMedPrepared = fromPubMed(source),
          pubMed = RemoveColumnsSafe(pubMedPrepared, {"ChEMBL.document_chembl_id"}),
          chembl = fromChEMBL(source),
          scholar = fromScholar(source),
          openAlex = fromOpenAlex(source),
          crossRefBase = fromCrossRef(source),
          crossRef = TransformColumnTypesSafe(Table.DuplicateColumn(crossRefBase, "PMID", "crossref.PMID"), {{"crossref.PMID", type text}}),
          joinChEMBL = Table.NestedJoin(pubMed, {"PMID"}, chembl, {"PMID"}, "ChEMBL", JoinKind.LeftOuter),
          withChEMBL = ExpandTableColumnSafe(
            joinChEMBL,
            "ChEMBL",
            {"title", "abstract", "ChEMBL.doi", "volume", "issue", "page", "authors", "document_chembl_id"},
            {"ChEMBL.title", "ChEMBL.abstract", "ChEMBL.doi", "ChEMBL.volume", "ChEMBL.issue", "ChEMBL.page", "authors", "ChEMBL.document_chembl_id"}
          ),
          joinScholar = Table.NestedJoin(withChEMBL, {"PMID"}, scholar, {"scholar.PMID"}, "Scholar", JoinKind.LeftOuter),
          withScholar = ExpandTableColumnSafe(
            joinScholar,
            "Scholar",
            {"scholar.doi", "scholar.PublicationTypes", "scholar.Venue", "scholar.Error"}
          ),
          joinOpenAlex = Table.NestedJoin(withScholar, {"PMID"}, openAlex, {"OpenAlex.PMID"}, "OpenAlex", JoinKind.LeftOuter),
          withOpenAlex = ExpandTableColumnSafe(
            joinOpenAlex,
            "OpenAlex",
            {"OpenAlex.doi", "publication_type", "crossref_type", "OpenAlex.Genre", "OpenAlex.Venue", "MeSH.descriptors", "Error"},
            {"OpenAlex.doi", "OpenAlex.publication_type", "OpenAlex.crossref_type", "OpenAlex.Genre", "OpenAlex.Venue", "OpenAlex.MeSH.descriptors", "OpenAlex.Error"}
          ),
          joinCrossRef = Table.NestedJoin(withOpenAlex, {"PMID"}, crossRef, {"crossref.PMID"}, "CrossRef", JoinKind.LeftOuter),
          merged = ExpandTableColumnSafe(
            joinCrossRef,
            "CrossRef",
            {"crossref.doi", "publication_type", "crossref.Subtype", "title", "crossref.Subtitle", "crossref.Subject", "Error"},
            {"crossref.doi", "crossref.publication_type", "crossref.crossref.Subtype", "crossref.title", "crossref.crossref.Subtitle", "crossref.crossref.Subject", "crossref.Error"}
          )
        in
          merged
      in
        [
          FromPubMed = fromPubMed,
          FromScholar = fromScholar,
          FromChEMBL = fromChEMBL,
          FromOpenAlex = fromOpenAlex,
          FromCrossRef = fromCrossRef,
          MergeSources = mergeSources
        ],
// =====================================================
// Modules (validation)
// =====================================================
  Validation =
    let
      // ===== Normalization helpers =====
      textParams = Parameters[TextCleanup],
      doiParams = Parameters[DoiNormalization],
      NormalizeText = (value as any) as nullable text => NormalizeWhitespace(value),
      IsLikelyDoi = (value as any) as logical =>
        let
          candidate = NormalizeDoi(value)
        in
          candidate <> null
            and Text.StartsWith(candidate, doiParams[AuthorityPrefix])
            and Text.Contains(candidate, doiParams[Separator])
            and not Text.Contains(candidate, textParams[Space])
            and Text.Length(candidate) >= doiParams[MinLength]
            and Text.Length(candidate) <= doiParams[MaxLength],
      HasValue = (value as any) as logical => NormalizeWhitespace(value) <> null,
      ListMode = (lst as list, optional ranker as nullable function) as record =>
        let
          nonNull = List.RemoveNulls(lst),
          distinctValues = List.Distinct(nonNull),
          counts = List.Transform(distinctValues, (v) => [value = v, count = List.Count(List.Select(nonNull, each _ = v))]),
          decorated =
            if ranker = null then
              counts
            else
              List.Transform(counts, (r) => Record.AddField(r, "rank", ranker(r[value]))),
          sorted =
            if ranker = null then
              List.Sort(decorated, (a, b) => Number.From(b[count]) - Number.From(a[count]))
            else
              List.Sort(
                decorated,
                (a, b) =>
                  let
                    countDiff = Number.From(b[count]) - Number.From(a[count]),
                    rankA = if Record.HasFields(a, "rank") then Number.From(a[rank]) else 0,
                    rankB = if Record.HasFields(b, "rank") then Number.From(b[rank]) else 0
                  in
                    if countDiff <> 0 then countDiff else rankA - rankB
              ),
          top = if List.Count(sorted) = 0 then [value = null, count = 0] else sorted{0}
        in
          top,
      CheckDoi = (pm as nullable text, chembl as nullable text, scholar as nullable text, crossref as nullable text, openalex as nullable text) as record =>
        let
          // ===== DOI source priority =====
          // 0 = PubMed, 1 = CrossRef, 2 = OpenAlex, 3 = ChEMBL, 4 = Scholar
          sourcePriority = {
            [key = "pm", name = "PubMed", priority = 0],
            [key = "crossref", name = "crossref", priority = 1],
            [key = "openalex", name = "OpenAlex", priority = 2],
            [key = "chembl", name = "ChEMBL", priority = 3],
            [key = "scholar", name = "scholar", priority = 4]
          },
          fallbackPriority = List.Max(List.Transform(sourcePriority, each Number.From(_[priority]))) + 1,
          rawMap = [pm = pm, chembl = chembl, scholar = scholar, crossref = crossref, openalex = openalex],
          sources =
            List.Transform(
              sourcePriority,
              (spec) =>
                let
                  rawValue = Record.Field(rawMap, spec[key]),
                  normValue = NormalizeDoi(rawValue),
                  isValid = IsLikelyDoi(normValue)
                in
                  spec & [raw = rawValue, norm = normValue, valid = isValid]
            ),
          sourcesByKey = Record.FromList(sources, List.Transform(sources, each _[key])),
          pmEntry = Record.Field(sourcesByKey, "pm"),
          validSources = List.Select(sources, each _[valid] and _[norm] <> null),
          getPriorityForDoi = (doi as nullable text) as number =>
            if doi = null then
              fallbackPriority
            else
              let
                matches = List.Transform(List.Select(validSources, each _[norm] = doi), each Number.From(_[priority]))
              in
                if List.Count(matches) = 0 then fallbackPriority else List.Min(matches),
          // Consensus DOI favors the most supported value; ties break by the best source priority defined above.
          consensusRec = ListMode(List.Transform(validSources, each _[norm]), getPriorityForDoi),
          consensusDoi = if Record.HasFields(consensusRec, "value") then consensusRec[value] else null,
          consensusSupport = if Record.HasFields(consensusRec, "count") then Number.From(consensusRec[count]) else 0,
          consensusSource =
            if consensusDoi = null then
              null
            else
              let
                supporters = List.Select(validSources, each _[norm] = consensusDoi),
                sortedSupporters = List.Sort(supporters, (a, b) => Number.From(a[priority]) - Number.From(b[priority]))
              in
                if List.Count(sortedSupporters) = 0 then null else sortedSupporters{0}[name],
          sortedValid = List.Sort(validSources, (a, b) => Number.From(a[priority]) - Number.From(b[priority])),
          selectedCandidate =
            if pmEntry[valid] then
              [doi = pmEntry[norm], source = pmEntry[name]]
            else if consensusSupport >= 2 and consensusDoi <> null then
              [doi = consensusDoi, source = consensusSource]
            else if List.Count(sortedValid) > 0 then
              [doi = sortedValid{0}[norm], source = sortedValid{0}[name]]
            else
              [doi = null, source = null],
          selectedDoi = selectedCandidate[doi],
          selectedSource = selectedCandidate[source],
          peerNormalized = List.RemoveNulls(List.Transform(List.Select(sources, each _[key] <> "pm"), each _[norm])),
          peerSources = List.Select(validSources, each _[key] <> "pm"),
          hasPeerSupport = List.Count(peerSources) > 0,
          pmMatchesPeers = if pmEntry[norm] = null then false else List.AnyTrue(List.Transform(peerSources, each _[norm] = pmEntry[norm])),
          // invalid_doi is raised when PubMed lacks a valid DOI while trusted peers provide one,
          // or when PubMed supplies a DOI that conflicts with valid peer sources.
          invalidDoi = (not pmEntry[valid] and hasPeerSupport) or (pmEntry[valid] and hasPeerSupport and not pmMatchesPeers),
          reason =
            if not pmEntry[valid] and hasPeerSupport then
              "pubmed_doi_missing_or_malformed"
            else if pmEntry[valid] and hasPeerSupport and not pmMatchesPeers then
              "pubmed_doi_mismatch_with_sources"
            else if pmEntry[valid] and pmMatchesPeers then
              "pubmed_doi_confirmed"
            else
              "insufficient_data",
          sameCount =
            if pmEntry[norm] = null then
              0
            else
              List.Count(List.Select(peerNormalized, each _ = pmEntry[norm])),
          peersValidDistinct = List.Distinct(List.Transform(peerSources, each _[norm]))
        in
          [
            invalid_doi = invalidDoi,
            reason = reason,
            same_count = sameCount,
            selected_doi = selectedDoi,
            selected_source = selectedSource,
            consensus_doi = consensusDoi,
            consensus_support = consensusSupport,
            pm_doi_norm = pmEntry[norm],
            pm_valid = pmEntry[valid],
            chembl_doi_norm = Record.Field(sourcesByKey, "chembl")[norm],
            chembl_valid = Record.Field(sourcesByKey, "chembl")[valid],
            scholar_doi_norm = Record.Field(sourcesByKey, "scholar")[norm],
            scholar_valid = Record.Field(sourcesByKey, "scholar")[valid],
            crossref_doi_norm = Record.Field(sourcesByKey, "crossref")[norm],
            crossref_valid = Record.Field(sourcesByKey, "crossref")[valid],
            openalex_doi_norm = Record.Field(sourcesByKey, "openalex")[norm],
            openalex_valid = Record.Field(sourcesByKey, "openalex")[valid],
            pm_doi_raw = pmEntry[raw],
            chembl_doi_raw = Record.Field(sourcesByKey, "chembl")[raw],
            scholar_doi_raw = Record.Field(sourcesByKey, "scholar")[raw],
            crossref_doi_raw = Record.Field(sourcesByKey, "crossref")[raw],
            openalex_doi_raw = Record.Field(sourcesByKey, "openalex")[raw],
            peers_valid_distinct = peersValidDistinct
          ],
      CheckTitle = (pm as nullable text, chembl as nullable text, crossref as nullable text) as record =>
        let
          pmN = NormalizeText(pm),
          chN = NormalizeText(chembl),
          crN = NormalizeText(crossref),
          same = if pmN = null then 0 else List.Count(List.Select(List.RemoveNulls({chN, crN}), each _ = pmN)),
          pmHasValue = HasValue(pm),
          crossrefHasValue = HasValue(crossref),
          newValue = if pmHasValue then pm else if crossrefHasValue then crossref else chembl
        in
          [same_count = same, new_title = newValue],
      CheckAbstract = (pm as nullable text, chembl as nullable text) as record =>
        let
          pmN = NormalizeText(pm),
          chN = NormalizeText(chembl),
          same = if pmN = null then 0 else (if chN = pmN then 1 else 0),
          newValue = if HasValue(pm) then pm else chembl
        in
          [same_count = same, new_abstract = newValue],
      CheckPages = (pm as nullable text, chembl as nullable text) as record =>
        let
          pmN = NormalizePages(pm),
          chN = NormalizePages(chembl),
          same = if pmN = null then 0 else (if chN = pmN then 1 else 0),
          newValue = if HasValue(pm) then pm else chembl
        in
          [same_count = same, new_page = newValue],
      CheckVolume = (pm as any, chembl as any) as record =>
        let
          pmNum = TryNumber(pm),
          chNum = TryNumber(chembl),
          pmHas = HasValue(pm),
          chHas = HasValue(chembl),
          same = if pmNum = null then 0 else (if chNum <> null and chNum = pmNum then 1 else 0),
          invalidVolume = (pmHas and pmNum = null) or (chHas and chNum = null) or (pmHas and chHas and pmNum <> chNum),
          newValue = if pmNum <> null then pmNum else chNum
        in
          [same_count = same, new_volume = newValue, invalid_volume = invalidVolume],
      CheckIssue = (pm as any, chembl as any) as record =>
        let
          pmNum = TryNumber(pm),
          chNum = TryNumber(chembl),
          pmHas = HasValue(pm),
          chHas = HasValue(chembl),
          same = if pmNum = null then 0 else (if chNum <> null and chNum = pmNum then 1 else 0),
          invalidIssue = (pmHas and pmNum = null) or (chHas and chNum = null) or (pmHas and chHas and pmNum <> chNum),
          newValue = if pmNum <> null then pmNum else chNum
        in
          [same_count = same, new_issue = newValue, invalid_issue = invalidIssue],
      ValidateRow = (row as record) as record =>
        let
          get = (name as text) => if Record.HasFields(row, name) then Record.Field(row, name) else null,
          doiRes = CheckDoi(get("PubMed.doi"), get("ChEMBL.doi"), get("scholar.doi"), get("crossref.doi"), get("OpenAlex.doi")),
          titleRes = CheckTitle(get("title"), get("ChEMBL.title"), get("crossref.title")),
          abstractRes = CheckAbstract(get("abstract"), get("ChEMBL.abstract")),
          pageRes = CheckPages(get("page"), get("ChEMBL.page")),
          volumeRes = CheckVolume(get("volume"), get("ChEMBL.volume")),
          issueRes = CheckIssue(get("issue"), get("ChEMBL.issue")),
          result = Record.Combine({[PMID = get("PMID")], doiRes, titleRes, abstractRes, pageRes, volumeRes, issueRes})
        in
          result,
      ValidateAll = (tableToCheck as table) as table =>
        let
          withValidation = Table.AddColumn(tableToCheck, "validation", each ValidateRow(Record.FromList(Record.ToList(_), Record.FieldNames(_)))),
          expanded = Table.ExpandRecordColumn(
            withValidation,
            "validation",
            {
              "same_count",
              "invalid_doi",
              "reason",
              "selected_doi",
              "selected_source",
              "consensus_doi",
              "consensus_support",
              "pm_doi_norm",
              "pm_valid",
              "chembl_doi_norm",
              "chembl_valid",
              "scholar_doi_norm",
              "scholar_valid",
              "crossref_doi_norm",
              "crossref_valid",
              "openalex_doi_norm",
              "openalex_valid",
              "pm_doi_raw",
              "chembl_doi_raw",
              "scholar_doi_raw",
              "crossref_doi_raw",
              "openalex_doi_raw",
              "peers_valid_distinct",
              "new_title",
              "new_abstract",
              "new_page",
              "new_volume",
              "invalid_volume",
              "new_issue",
              "invalid_issue",
              "PMID"
            },
            {
              "doi_same_count",
              "invalid_doi",
              "reason",
              "selected_doi",
              "selected_source",
              "consensus_doi",
              "consensus_support",
              "pm_doi_norm",
              "pm_valid",
              "chembl_doi_norm",
              "chembl_valid",
              "scholar_doi_norm",
              "scholar_valid",
              "crossref_doi_norm",
              "crossref_valid",
              "openalex_doi_norm",
              "openalex_valid",
              "pm_doi_raw",
              "chembl_doi_raw",
              "scholar_doi_raw",
              "crossref_doi_raw",
              "openalex_doi_raw",
              "peers_valid_distinct",
              "new_title",
              "new_abstract",
              "new_page",
              "new_volume",
              "invalid_volume",
              "new_issue",
              "invalid_issue",
              "PMID_for_validation"
            }
          ),
          typed = TransformColumnTypesSafe(
            expanded,
            {
              {"new_volume", type nullable number},
              {"new_issue", type nullable number},
              {"invalid_volume", type logical},
              {"invalid_issue", type logical}
            }
          )
        in
          typed
    in
      [
        NormalizeText = NormalizeText,
        NormalizeDoi = NormalizeDoi,
        IsLikelyDoi = IsLikelyDoi,
        NormalizePages = NormalizePages,
        TryNumber = TryNumber,
        ListMode = ListMode,
        CheckDoi = CheckDoi,
        CheckTitle = CheckTitle,
        CheckAbstract = CheckAbstract,
        CheckPages = CheckPages,
        CheckVolume = CheckVolume,
        CheckIssue = CheckIssue,
        ValidateRow = ValidateRow,
        ValidateAll = ValidateAll
      ],
  Modules =
    let
      BuildValidationFrame = (validated as table) as table =>
        let
          invalidFlag = Table.AddColumn(
            validated,
            "invalid_record",
            each ([consensus_support] <= 2) or ([invalid_issue] = true) or ([invalid_volume] = true),
            type logical
          ),
          withoutOriginals = Table.RemoveColumns(
            invalidFlag,
            {"title", "abstract", "volume", "issue", "page"},
            MissingField.Ignore
          ),
          withoutNoise = Table.RemoveColumns(
            withoutOriginals,
            {"ChEMBL.doi", "scholar.doi", "OpenAlex.doi", "crossref.doi"},
            MissingField.Ignore
          ),
          renamedDraft = Table.RenameColumns(
            withoutNoise,
            {
              {"new_title", "_title"},
              {"new_abstract", "abstract_"},
              {"new_volume", "volume"},
              {"new_issue", "issue"},
              {"new_page", "page"},
              {"selected_doi", "doi"}
            },
            MissingField.Ignore
          ),
          withCompleted = Table.AddColumn(
            renamedDraft,
            "completed",
            each
              let
                cy = Text.PadStart(Text.From([completed.year]), 4, "0"),
                cm = Text.PadStart(Text.From([completed.month]), 2, "0"),
                cd = Text.PadStart(Text.From([completed.day]), 2, "0"),
                ry = Text.PadStart(Text.From([revised.year]), 4, "0"),
                rm = Text.PadStart(Text.From([revised.month]), 2, "0"),
                rd = Text.PadStart(Text.From([revised.day]), 2, "0"),
                hasCompleted = (cy <> "0000") and (cm <> "00") and (cd <> "00")
              in
                if hasCompleted then cy & "-" & cm & "-" & cd else ry & "-" & rm & "-" & rd,
            type text
          ),
          withSort = Table.AddColumn(
            withCompleted,
            "sort_order",
            each [ISSN] & ":" & [completed] & ":" & Text.PadStart(Text.From([PMID]), 8, "0"),
            type text
          ),
          withoutVerbose = Table.RemoveColumns(
            withSort,
            {
              "doi_same_count",
              "invalid_doi",
              "reason",
              "consensus_doi",
              "consensus_support",
              "pm_doi_norm",
              "pm_valid",
              "chembl_doi_norm",
              "chembl_valid",
              "scholar_doi_norm",
              "scholar_valid",
              "crossref_doi_norm",
              "crossref_valid",
              "openalex_doi_norm",
              "openalex_valid",
              "pm_doi_raw",
              "chembl_doi_raw",
              "scholar_doi_raw",
              "crossref_doi_raw",
              "openalex_doi_raw",
              "peers_valid_distinct"
            },
            MissingField.Ignore
          ),
          ordered = Table.ReorderColumns(withoutVerbose, List.Sort(Table.ColumnNames(withoutVerbose)))
        in
          ordered,


      // ===== BuildDocumentTable =====
      // Inputs:
      //   - DocumentInput[MergeSources]: объединяет данные документа по всем источникам.
      //   - Validation[ValidateAll]: рассчитывает нормализованные атрибуты документа.
      //   - citations[get_reference], citations[get_citations_fraction]: добавляют классификации и агрегаты цитирования.
      // Output:
      //   - Таблица документов с согласованной схемой и типами для downstream-потребителей.
      BuildDocumentTable = () as table =>
        let
          documentSource = DocumentInput[MergeSources](Data[Document_out]),
          validatedRows = Validation[ValidateAll](documentSource),
          validationFrame = BuildValidationFrame(validatedRows),
          pmidPrepared = Helpers[TransformColumnTypesSafe](validationFrame, {{"PMID", type text}}),
          referenceTable = citations[get_reference](),
          withReference = Table.NestedJoin(
            pmidPrepared,
            {"PMID"},
            referenceTable,
            {"pubmed_id"},
            "DocumentReference",
            JoinKind.LeftOuter
          ),
          expandedReference = ExpandTableColumnSafe(
            withReference,
            "DocumentReference",
            {"classification", "document_contains_external_links", "is_experimental_doc"},
            {"review", "document_contains_external_links", "is_experimental_doc"}
          ),
          reviewSanitized =
            let
              replaced = Table.ReplaceValue(expandedReference, "", 0, Replacer.ReplaceValue, {"review"}),
              typedNumber = Helpers[TransformColumnTypesSafe](replaced, {{"review", Int64.Type}})
            in
              Helpers[TransformColumnTypesSafe](typedNumber, {{"review", type logical}}),
          ensuredDocumentId = EnsureColumn(reviewSanitized, "ChEMBL.document_chembl_id", null, type text),
          metricsTable = citations[get_citations_fraction](),
          withMetrics = Table.NestedJoin(
            ensuredDocumentId,
            {"ChEMBL.document_chembl_id"},
            metricsTable,
            {"document_chembl_id"},
            "DocumentMetrics",
            JoinKind.LeftOuter
          ),
          expandedMetrics = ExpandTableColumnSafe(
            withMetrics,
            "DocumentMetrics",
            {"n_activity", "citations", "n_assay", "n_testitem", "significant_citations_fraction"}
          ),
          dropJournalish = {"journal-article", "journal article", "article", "journal"},
          dropSupportish =
            {
              "research support, u.s. gov't, p.h.s.",
              "research support, non-u.s. gov't",
              "research support, u.s. gov't, non-p.h.s.",
              "research support, n.i.h., extramural",
              "research support, n.i.h., intramural",
              "research support, american recovery and reinvestment act"
            },
          aliasPublicationType =
            [
              #"clinical trial, phase i" = "clinical trial",
              #"clinical trial, phase ii" = "clinical trial",
              #"historical article" = "review",
              #"validation study" = "validation study",
              lecture = "review",
              address = "review",
              #"comparative study" = "comparative study"
            ],
          aliasScholar = [study = null, clinicaltrial = "clinicaltrial", review = "review", lettersandcomments = "lettersandcomments"],
          aliasOpenAlexPubType = [paratext = "paratext", component = "paratext", article = null, journal = null, #"journal-article" = null, #"journal article" = null],
          aliasOpenAlexXrefType = [#"journal-article" = null, article = null],
          aliasOpenAlexGenre = [article = null, #"0" = null],
          aliasCrossrefPubType = [#"journal-article" = null, article = null],
          classificationConfigs =
            {
              [Column = "publication_type", Alias = aliasPublicationType, Drop = List.Union({dropJournalish, dropSupportish, {""}})],
              [Column = "scholar.PublicationTypes", Alias = aliasScholar, Drop = {"journalarticle", "study", ""}],
              [Column = "OpenAlex.publication_type", Alias = aliasOpenAlexPubType, Drop = {"0", "article", "journal", "journal-article", "journal article", ""}],
              [Column = "OpenAlex.crossref_type", Alias = aliasOpenAlexXrefType, Drop = {"journal-article", "article", ""}],
              [Column = "OpenAlex.Genre", Alias = aliasOpenAlexGenre, Drop = {"0", "article", ""}],
              [Column = "crossref.publication_type", Alias = aliasCrossrefPubType, Drop = {"journal-article", "article", ""}]
            },
          classificationNormalized =
            List.Accumulate(
              classificationConfigs,
              expandedMetrics,
              (state as table, cfg as record) =>
                Table.TransformColumns(
                  state,
                  {{cfg[Column], each CleanPipe(_, cfg[Alias], cfg[Drop], false), type text}}
                )
            ),
          reviewSignals = [BaseWeight = 2, Threshold = 0.335],
          responseColumns = {"publication_type", "scholar.PublicationTypes", "OpenAlex.publication_type", "OpenAlex.crossref_type"},
          withResponseCount =
            Table.AddColumn(
              classificationNormalized,
              "n_responces",
              each
                let
                  currentRow = _,
                  responses =
                    List.Transform(
                      responseColumns,
                      (columnName as text) =>
                        let
                          value = Record.FieldOrDefault(currentRow, columnName, null),
                          textValue = ToText(value)
                        in
                          if textValue <> "" then 1 else 0
                    )
                in
                  reviewSignals[BaseWeight] + List.Sum(responses),
              Int64.Type
            ),
          withReview =
            Table.AddColumn(
              withResponseCount,
              "updated_review",
              each
                let
                  pubType = ToText(Record.FieldOrDefault(_, "publication_type", "")),
                  scholarType = ToText(Record.FieldOrDefault(_, "scholar.PublicationTypes", "")),
                  openAlexType = ToText(Record.FieldOrDefault(_, "OpenAlex.publication_type", "")),
                  openAlexXref = ToText(Record.FieldOrDefault(_, "OpenAlex.crossref_type", "")),
                  baseReview = Record.FieldOrDefault(_, "review", false),
                  voteScore =
                    Number.From(Text.Contains(pubType, "review")) +
                    Number.From(scholarType = "review") +
                    Number.From(openAlexType = "review") +
                    Number.From(openAlexXref = "review") +
                    Number.From(baseReview) * reviewSignals[BaseWeight],
                  normalizedScore = voteScore / Record.Field(_, "n_responces")
                in
                  baseReview or normalizedScore > reviewSignals[Threshold],
              type logical
            ),
          reviewFinal =
            let
              withoutLegacy = RemoveColumnsSafe(withReview, {"review"})
            in
              RenameColumnsSafe(withoutLegacy, {{"updated_review", "review"}}),
          withExperimentalFlag = Table.AddColumn(reviewFinal, "is_experimental", each not [review], type logical),
          documentSchema = [
            Rename = {
              {"_title", "title"},
              {"abstract_", "abstract"},
              {"MeSH.descriptors", "PubMed.MeSH"},
              {"OpenAlex.MeSH.descriptors", "OpenAlex.MeSH"},
              {"PubMed.MeSH_Qualifiers", "MeSH.qualifiers"},
              {"PubMed.ChemicalList", "chemical_list"},
              {"publication_type", "PubMed.publication_type"}
            },
            Type = {
              {"PMID", Int64.Type},
              {"doi", type text},
              {"sort_order", type text},
              {"completed", type text},
              {"invalid_record", type logical},
              {"title", type text},
              {"abstract", type text},
              {"authors", type text},
              {"PubMed.MeSH", type text},
              {"OpenAlex.MeSH", type text},
              {"MeSH.qualifiers", type text},
              {"chemical_list", type text},
              {"ChEMBL.document_chembl_id", type text},
              {"PubMed.publication_type", type text},
              {"scholar.PublicationTypes", type text},
              {"OpenAlex.publication_type", type text},
              {"OpenAlex.crossref_type", type text},
              {"OpenAlex.Genre", type text},
              {"crossref.publication_type", type text},
              {"significant_citations_fraction", type logical},
              {"document_contains_external_links", type logical},
              {"is_experimental_doc", type logical},
              {"n_activity", Int64.Type},
              {"citations", Int64.Type},
              {"n_assay", Int64.Type},
              {"n_testitem", Int64.Type},
              {"n_responces", Int64.Type},
              {"review", type logical},
              {"is_experimental", type logical}
            },
            Order = {
              "PMID",
              "doi",
              "sort_order",
              "completed",
              "invalid_record",
              "title",
              "abstract",
              "authors",
              "PubMed.MeSH",
              "OpenAlex.MeSH",
              "MeSH.qualifiers",
              "chemical_list",
              "ChEMBL.document_chembl_id",
              "PubMed.publication_type",
              "scholar.PublicationTypes",
              "OpenAlex.publication_type",
              "OpenAlex.crossref_type",
              "OpenAlex.Genre",
              "crossref.publication_type",
              "significant_citations_fraction",
              "document_contains_external_links",
              "is_experimental_doc",
              "n_activity",
              "citations",
              "n_assay",
              "n_testitem",
              "n_responces",
              "review",
              "is_experimental"
            }
          ],
          ApplyDocumentSchema = (tbl as table) as table =>
            let
              renamed = RenameColumnsSafe(tbl, documentSchema[Rename]),
              typed = Helpers[TransformColumnTypesSafe](renamed, documentSchema[Type]),
              selected = SelectColumnsSafe(typed, documentSchema[Order]),
              ordered = Table.ReorderColumns(selected, documentSchema[Order], MissingField.Ignore)
            in
              ordered,
          finalTable = ApplyDocumentSchema(withExperimentalFlag)
        in
          finalTable,

      // ===== BuildTestitemTable =====
      // Назначение: нормализация тестируемых сущностей и присоединение агрегатов документа.
      BuildTestitemTable = () as table =>
        let
          textParams = Parameters[TextCleanup],
          pipeSeparator = textParams[PipeSeparator],
          referenceRaw = Data[Testitem_in],
          referenceSchema = {
            {"molecule_chembl_id", type text},
            {"all_names", type text},
            {"nstereo", Int64.Type}
          },
          referenceTyped = TransformColumnTypesSafe(referenceRaw, referenceSchema),
          sourceRaw = Data[Testitem_out],
          sourceSchema = {
            {"molecule_chembl_id", type text},
            {"pref_name", type text},
            {"synonyms", type text},
            {"molecule_type", type text},
            {"structure_type", type text},
            {"is_radical", type logical},
            {"standard_inchi_key", type text},
            {"document_chembl_id", type text}
          },
          sourceTyped = TransformColumnTypesSafe(sourceRaw, sourceSchema),
          withReference = JoinAndExpand(
            sourceTyped,
            {"molecule_chembl_id"},
            referenceTyped,
            {"molecule_chembl_id"},
            "TestitemReference",
            {"all_names", "nstereo"},
            {"all_names", "nstereo"}
          ),
          lowercaseText = LowercaseColumns(withReference, {"synonyms", "all_names", "pref_name"}),
          doubleQuote = Character.FromNumber(34),
          openToken = "[" & doubleQuote,
          closeToken = doubleQuote & "]",
          synonymReplacements = {
            [Old = closeToken, New = ""],
            [Old = openToken, New = ""]
          },
          synonymsSanitized = ReplaceTextInColumn(lowercaseText, "synonyms", synonymReplacements),
          synonymsPiped = Table.TransformColumns(
            synonymsSanitized,
            {{"synonyms", each if _ = null then null else Text.Replace(_, doubleQuote, pipeSeparator), type nullable text}}
          ),
          chiralityShifted = Table.TransformColumns(
            synonymsPiped,
            {{"nstereo", each if _ = null then null else _ - 1, Int64.Type}}
          ),
          chiralityFlag = Table.TransformColumns(
            chiralityShifted,
            {{"nstereo", each if _ = null then false else _ <> 0, type logical}}
          ),
          renamedChirality = Table.RenameColumns(chiralityFlag, {{"nstereo", "unknown_chirality"}}, MissingField.Ignore),
          textColumnsTyped = TransformColumnTypesSafe(
            renamedChirality,
            {{"synonyms", type text}, {"all_names", type text}, {"pref_name", type text}, {"unknown_chirality", type logical}}
          ),
          withDocumentId = EnsureColumn(textColumnsTyped, "document_chembl_id", null, type text),
          documentAggRaw = citations[get_testitem_agg](),
          documentAggTyped = TransformColumnTypesSafe(
            documentAggRaw,
            {{"document_chembl_id", type text}, {"n_testitem", Int64.Type}}
          ),
          withAggregates = JoinAndExpand(
            withDocumentId,
            {"document_chembl_id"},
            documentAggTyped,
            {"document_chembl_id"},
            "DocumentTestitemAgg",
            {"n_testitem"},
            {"document_testitem_total"}
          ),
          aggregatesFinal = FinalizeAggColumns(withAggregates, {"document_testitem_total"}),
          withInvalidFlag = Table.AddColumn(
            aggregatesFinal,
            "invalid_record",
            each not (
              [molecule_type] = "Small molecule"
                and [structure_type] = "MOL"
                and [is_radical] = false
                and [standard_inchi_key] <> ""
            ),
            type logical
          ),
          columnOrder = {
            "molecule_chembl_id",
            "pref_name",
            "all_names",
            "synonyms",
            "molecule_type",
            "structure_type",
            "is_radical",
            "standard_inchi_key",
            "unknown_chirality",
            "document_chembl_id",
            "document_testitem_total",
            "invalid_record"
          },
          finalTestitem = Table.ReorderColumns(withInvalidFlag, columnOrder, MissingField.Ignore)
        in
          finalTestitem,

      // ===== BuildAssayTable =====
      // Назначение: типизация таблицы биологических тестов и добавление агрегатов по документу.
      BuildAssayTable = () as table =>
        let
          sourceRaw = Data[Assay_out],
          typeMap = {
            {"assay_chembl_id", type text},
            {"document_chembl_id", type text},
            {"target_chembl_id", type text},
            {"assay_category", type text},
            {"assay_group", type text},
            {"assay_type", type text},
            {"assay_type_description", type text},
            {"assay_organism", type text},
            {"assay_test_type", type text},
            {"assay_cell_type", type text},
            {"assay_tissue", type text},
            {"assay_tax_id", Int64.Type},
            {"assay_with_same_target", Int64.Type},
            {"confidence_score", Int64.Type},
            {"confidence_description", type text},
            {"relationship_type", type text},
            {"relationship_description", type text},
            {"bao_format", type text},
            {"bao_label", type text},
            {"aidx", type text},
            {"assay_classifications", type text},
            {"assay_parameters", type text},
            {"assay_strain", type text},
            {"assay_subcellular_fraction", type text},
            {"cell_chembl_id", type text},
            {"description", type text},
            {"src_assay_id", type text},
            {"src_id", Int64.Type},
            {"tissue_chembl_id", type text},
            {"variant_sequence", type text}
          },
          typed = TransformColumnTypesSafe(sourceRaw, typeMap),
          withDocumentId = EnsureColumn(typed, "document_chembl_id", null, type text),
          documentAggRaw = citations[get_assay_agg](),
          documentAggTyped = TransformColumnTypesSafe(
            documentAggRaw,
            {{"document_chembl_id", type text}, {"n_assay", Int64.Type}}
          ),
          withAggregates = JoinAndExpand(
            withDocumentId,
            {"document_chembl_id"},
            documentAggTyped,
            {"document_chembl_id"},
            "DocumentAssayAgg",
            {"n_assay"},
            {"document_assay_total"}
          ),
          aggregatesFinal = FinalizeAggColumns(withAggregates, {"document_assay_total"}),
          columnsToRemove = {
            "assay_tax_id",
            "confidence_score",
            "confidence_description",
            "relationship_type",
            "relationship_description",
            "assay_strain"
          },
          sanitized = RemoveColumnsSafe(aggregatesFinal, columnsToRemove),
          columnOrderAssay = {
            "assay_chembl_id",
            "document_chembl_id",
            "target_chembl_id",
            "assay_category",
            "assay_group",
            "assay_type",
            "assay_type_description",
            "assay_organism",
            "assay_test_type",
            "assay_cell_type",
            "assay_tissue",
            "assay_with_same_target",
            "bao_format",
            "bao_label",
            "aidx",
            "assay_classifications",
            "assay_parameters",
            "assay_subcellular_fraction",
            "cell_chembl_id",
            "description",
            "src_assay_id",
            "src_id",
            "tissue_chembl_id",
            "variant_sequence",
            "document_assay_total"
          },
          finalAssay = Table.ReorderColumns(sanitized, columnOrderAssay, MissingField.Ignore)
        in
          finalAssay


    in
      [
        BuildValidationFrame = BuildValidationFrame,
        BuildDocumentTable = BuildDocumentTable,
        BuildTestitemTable = BuildTestitemTable,
        BuildAssayTable = BuildAssayTable
      ],
// ===================== document_validation =====================
  data_validation_base = [
    // ===== get_validation =====
    // Назначение: валидация документарных записей и подготовка нормализованных атрибутов.
    get_validation = () as table =>
      let
        documentSource = Data[Document_out],
        mergedSources = DocumentInput[MergeSources](documentSource),
        validatedRows = Validation[ValidateAll](mergedSources),
        prepared = Modules[BuildValidationFrame](validatedRows)
      in
        prepared,

    // ===== get_document =====
    // Назначение: возврат очищенной таблицы документов для downstream-потребителей.
    get_document = () =>
      Modules[BuildDocumentTable](),
    // ===== get_testitem =====
    // Назначение: объединение тестируемых сущностей с эталонным справочником и расчёт флагов качества.
    get_testitem = () =>
      Modules[BuildTestitemTable](),
    // ===== get_assay =====
    // Назначение: типизация и очистка витрины биологических тестов.
    get_assay = () =>
      Modules[BuildAssayTable]()
  ],

  IUPHAR_int = () =>
    let
      #"Removed Columns" = DropColumns(
        Data[Target_out],
        {
          "isoform_ids",
          "isoform_names",
          "isoform_synonyms",
          "organism",
          "taxon_id",
          "lineage_superkingdom",
          "lineage_phylum",
          "lineage_class",
          "xref_chembl",
          "gtop_synonyms",
          "gtop_natural_ligands_n",
          "gtop_interactions_n",
          "gtop_function_text_short",
          "uniProtkbId",
          "hgnc_name",
          "secondaryAccessions",
          "transmembrane",
          "intramembrane",
          "hgnc_id",
          "features_signal_peptide",
          "tax_id",
          "species_group_flag",
          "family",
          "xref_uniprot",
          "xref_ensembl",
          "pfam",
          "interpro",
          "xref_pdb",
          "xref_alphafold",
          "SUPFAM",
          "PROSITE",
          "PRINTS",
          "TCDB",
          "target_type",
          "protein_classifications",
          "protein_name_alt",
          "recommendedName",
          "pref_name",
          "target_components",
          "cross_references",
          "gene_symbol_list",
          "protein_synonym_list",
          "ptm_glycosylation",
          "ptm_lipidation",
          "ptm_disulfide_bond",
          "ptm_modified_residue",
          "glycosylation",
          "lipidation",
          "disulfide_bond",
          "modified_residue",
          "phosphorylation",
          "acetylation",
          "ubiquitination",
          "signal_peptide",
          "propeptide",
          "reactions",
          "reaction_ec_numbers"
        }
      ),
      #"Reordered Columns" = Table.ReorderColumns(
        #"Removed Columns",
        {
          "target_chembl_id",
          "uniprot_id_primary",
          "secondaryAccessions",
          "gene_symbol",
          "protein_name_canonical",
          "sequence_length",
          "features_transmembrane",
          "features_topology",
          "xref_iuphar",
          "gtop_target_id",
          "uniprot_last_update",
          "uniprot_version",
          "pipeline_version",
          "timestamp_utc",
          "geneName",
          "molecular_function",
          "cellular_component",
          "subcellular_location",
          "topology",
          "GuidetoPHARMACOLOGY",
          "protein_class_pred_L1",
          "protein_class_pred_L2",
          "protein_class_pred_L3",
          "protein_class_pred_rule_id",
          "protein_class_pred_evidence",
          "protein_class_pred_confidence",
          "iuphar_target_id",
          "iuphar_family_id",
          "iuphar_type",
          "iuphar_class",
          "iuphar_subclass",
          "iuphar_chain",
          "iuphar_name",
          "iuphar_full_id_path",
          "iuphar_full_name_path"
        },
        MissingField.Ignore
      ),
      #"Removed Columns1" = DropColumns(
        #"Reordered Columns",
        {
          "uniprot_last_update",
          "uniprot_version",
          "pipeline_version",
          "timestamp_utc",
          "geneName",
          "secondaryAccessions",
          "gene_symbol",
          "protein_name_canonical",
          "sequence_length",
          "features_transmembrane",
          "features_topology",
          "molecular_function",
          "cellular_component",
          "subcellular_location",
          "topology",
          "xref_iuphar",
          "gtop_target_id",
          "GuidetoPHARMACOLOGY",
          "iuphar_name"
        }
      )
    in
      #"Removed Columns1",
  table19 = () as table =>
    let
      Rows =
        {
          {3, "n/a", "n/a", "Enzyme.Multifunctional", "Enzyme", "Multifunctional", "0690-3>0690", "n/a", "n/a", "Enzyme.Multifunctional", "Multifunctional"},
          {1, "n/a", "n/a", "Enzyme.Oxidoreductase", "Enzyme", "Oxidoreductase", "0690-1>0690", "n/a", "n/a", "Enzyme.Oxidoreductase", "Oxidoreductase"},
          {4, "n/a", "n/a", "Enzyme.Hydrolase", "Enzyme", "Hydrolase", "0690-4>0690", "n/a", "n/a", "Enzyme: Protease", "Protease"},
          {2, "n/a", "n/a", "Enzyme.Transferase", "Enzyme", "Transferase", "0690-2>0690", "n/a", "n/a", "Enzyme: Kinase", "Kinase"},
          {6, "n/a", "n/a", "Enzyme.Lyase", "Enzyme", "Lyase", "0690-6>0690", "n/a", "n/a", "Enzyme.Lyase", "Lyase"},
          {5, "n/a", "n/a", "Enzyme.Isomerase", "Enzyme", "Isomerase", "0690-5>0690", "n/a", "n/a", "Enzyme.Isomerase", "Isomerase"},
          {null, "n/a", "n/a", "Transporter.ATPase", "Transporter", "ATPase", "0865>0690", "n/a", "n/a", "Transporter: Other", "Transporter: Other"},
          {null, "n/a", "n/a", "Transporter.SLC superfamily of solute carrier", "Transporter", "SLC superfamily of solute carrier", "0863>0691", "n/a", "n/a", "Transporter: SLC", "SLC"},
          {null, "n/a", "n/a", "Transporter.ATP-binding cassette transporter", "Transporter", "ATP-binding cassette transporter", "0863>0691", "n/a", "n/a", "Transporter: ABC", "ABC"},
          {null, "n/a", "n/a", "Ion Channel.Voltage-gated ion channel", "Ion Channel", "Voltage-gated ion channel", "0696>0689", "n/a", "n/a", "Ion Channel: Voltage-gated", "Ion Channel: Voltage-gated"},
          {null, "n/a", "n/a", "Ion Channel.Ligand-gated ion channel", "Ion Channel", "Ligand-gated ion channel", "0697>0689", "n/a", "n/a", "Ion Channel: Ligand-gated", "Ion Channel: Ligand-gated"},
          {null, "n/a", "n/a", "Receptor.Nuclear Hormone Receptor", "Receptor", "Nuclear Hormone Receptor", "0695>0688", "n/a", "n/a", "Transcription factor", "TF: Other"},
          {null, "n/a", "n/a", "Receptor.G protein-coupled receptor", "Receptor", "G protein-coupled receptor", "0694>0688", "n/a", "n/a", "Receptor: GPCR", null},
          {100, "n/a", "n/a", "Other Protein Target.Other Protein Target", "Other Protein Target", "Other Protein Target", "0864-1>0864", "n/a", "n/a", "-", "-"},
          {null, "n/a", "n/a", "Receptor.Nuclear Hormone Receptor", "Receptor", "Nuclear Hormone Receptor", "0695>0688", "n/a", "n/a", "Transcription factor", "Zinc finger"}
        },
      Type =
        type table[
          ec = nullable Int64.Type,
          iuphar_target_id = nullable text,
          iuphar_family_id = nullable text,
          iuphar_type = nullable text,
          iuphar_class = nullable text,
          iuphar_subclass = nullable text,
          iuphar_chain = nullable text,
          iuphar_full_id_path = nullable text,
          iuphar_full_name_path = nullable text,
          Column1 = nullable text,
          Column2 = nullable text
        ],
      Tbl = Table.FromRows(Rows, Type)
    in
      Tbl,
// =====================================================
// Cellularity rules (офлайн)
// =====================================================
  Cellularity_ = [
    Normalize = (s as nullable text) as text => if s = null then "" else Text.Lower(Text.Trim(s)),
    UnicellPhyla =
      {
        "ciliophora",
        "apicomplexa",
        "euglenozoa",
        "dinoflagellata",
        "alveolata",
        "bacillariophyta",
        "parabasalia",
        "metamonada",
        "choanoflagellata",
        "chlorophyta",
""
      },
    MulticellAnimalPhyla =
      {
        "chordata",
        "arthropoda",
        "mollusca",
        "nematoda",
        "annelida",
        "echinodermata",
        "cnidaria",
        "platyhelminthes",
        "porifera",
        "ctenophora",
        "tardigrada",
        "onychophora",
        "bryozoa",
        "brachiopoda",
        "hemichordata",
        "rotifera",
        "nemertea",
        "sipuncula",
        "priapulida",
        "loricifera",
        "gastrotricha",
        "kinorhyncha",
        "spiralia",
              "ecdysozoa"
      },
    MulticellPlantPhyla = {"streptophyta", "tracheophyta", "bryophyta", "marchantiophyta"},
    MostlyMulticellPhyla = {"rhodophyta"},
    FungiPhyla = {"ascomycota", "basidiomycota", "mucoromycota", "microsporidia", "chytridiomycota", "dikarya"},
    ClassifyByLineage = (superkingdom as nullable text, phylum as nullable text) as text =>
      let
        sk = Normalize(superkingdom),
        ph = Normalize(phylum),
        res =
                if sk = "" then "unicellular"
          else if sk = "viruses" then
            "acellular (virus)"
          else if sk = "bacteria" or sk = "archaea" then
            "unicellular"
          else if sk = "eukaryota" then
            if List.Contains(MulticellAnimalPhyla, ph) or List.Contains(MulticellPlantPhyla, ph) then
              "multicellular"
            else if List.Contains(UnicellPhyla, ph) then
              "unicellular"
            else if List.Contains(FungiPhyla, ph) then
              "multicellular"
            else if List.Contains(MostlyMulticellPhyla, ph) then
              "multicellular"
            else
              "ambiguous"
          else
            "ambiguous"
      in
        res,
    AddCellularitySmart = (src as table, taxIdColumn as text, superkingdomColumn as text, phylumColumn as text) as table =>
      let
        ensureColumn = (tbl as table, columnName as text, columnType as type) as table =>
          if List.Contains(Table.ColumnNames(tbl), columnName) then
            tbl
          else
            Table.AddColumn(tbl, columnName, each null, columnType),
        preparedBase =
          List.Accumulate(
            {
              [Name = taxIdColumn, Type = Int64.Type],
              [Name = superkingdomColumn, Type = type nullable text],
              [Name = phylumColumn, Type = type nullable text]
            },
            src,
            (state as table, cfg as record) => ensureColumn(state, cfg[Name], cfg[Type])
          ),
        withoutCellularity =
          if List.Contains(Table.ColumnNames(preparedBase), "cellularity") then
            Table.RemoveColumns(preparedBase, {"cellularity"}, MissingField.Ignore)
          else
            preparedBase,
        columnNames = Table.ColumnNames(withoutCellularity),
        candidateRanks =
          List.Distinct(
            List.RemoveNulls(
              {
                if List.Contains(columnNames, phylumColumn) then phylumColumn else null,
                if phylumColumn <> "lineage_phylum" and List.Contains(columnNames, "lineage_phylum") then "lineage_phylum" else null,
                if List.Contains(columnNames, "lineage_class") then "lineage_class" else null
              }
            )
          ),
        toTextOrNull = (value as any) as nullable text =>
          if value = null then
            null
          else
            let
              primary = try Text.From(value, "en-US")
            in
              if primary[HasError] then
                let
                  fallback = try Text.From(value)
                in
                  if fallback[HasError] then null else fallback[Value]
              else
                primary[Value],
        classifyRow = (row as record) as text =>
          let
            superkingdomValue = toTextOrNull(Record.FieldOrDefault(row, superkingdomColumn, null)),
            results =
              if superkingdomValue = null then
                {}
              else
                List.Transform(
                  candidateRanks,
                  (rankColumn as text) =>
                    ClassifyByLineage(
                      superkingdomValue,
                      toTextOrNull(Record.FieldOrDefault(row, rankColumn, null))
                    )
                ),
            nonAmbiguous = List.Select(results, each _ <> "ambiguous"),
            resolved =
              if superkingdomValue = null then
                "ambiguous"
              else if not List.IsEmpty(nonAmbiguous) then
                nonAmbiguous{0}
              else if not List.IsEmpty(results) then
                results{0}
              else
                ClassifyByLineage(superkingdomValue, null)
          in
            resolved,
        withCellularity = Table.AddColumn(
          withoutCellularity,
          "cellularity",
          each classifyRow(_),
          type text
        )
      in
        withCellularity
  ],
// =====================================================
// Target modules (модуль функций)
// =====================================================
  TargetModules =
    let
      SelectColumns = SelectColumnsSafe,
      SelectInOrder = SelectColumnsInOrder,
      Drop = DropColumns,
      JoinExpand = JoinAndExpand,
      SplitPipe = SplitPipeList,
      PredCols = {
        "protein_class_pred_L1",
        "protein_class_pred_L2",
        "protein_class_pred_L3",
        "protein_class_pred_rule_id",
        "protein_class_pred_evidence",
        "protein_class_pred_confidence",
        "protein_classifications"
      },
      IupharIdCols = {
        "iuphar_target_id",
        "iuphar_family_id",
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "iuphar_chain",
        "iuphar_full_id_path",
        "iuphar_full_name_path"
      },
      IsoformModule =
        [
          Build = (source as table) as table =>
            let
              base = SelectColumns(
                source,
                {
                  "isoform_synonyms",
                  "isoform_names",
                  "isoform_ids",
                  "uniprot_id_primary",
                  "target_chembl_id"
                }
              ),
              normalized = Table.TransformColumns(
                base,
                {
                  {"isoform_synonyms", each Text.Lower(ToText(_)), type text},
                  {"isoform_names", each Text.Lower(ToText(_)), type text}
                }
              ),
              splitted = Table.TransformColumns(
                normalized,
                {
                  {"isoform_synonyms", each SplitPipe(_), type list},
                  {"isoform_names", each SplitPipe(_), type list},
                  {"isoform_ids", each SplitPipe(_), type list}
                }
              ),
              makeTriples = (names as list, ids as list, syns as list) as list =>
                let
                  count = List.Max({List.Count(names), List.Count(ids), List.Count(syns)}),
                  indexes = if count = 0 then {} else {0 .. count - 1}
                in
                  List.Transform(
                    indexes,
                    (i) =>
                      [
                        name = if i < List.Count(names) then names{i} else null,
                        id = if i < List.Count(ids) then ids{i} else null,
                        synonym = if i < List.Count(syns) then syns{i} else null
                      ]
                  ),
              withTriples = Table.AddColumn(
                splitted,
                "triples",
                each makeTriples([isoform_names], [isoform_ids], [isoform_synonyms]),
                type list
              ),
              rows = Table.ExpandListColumn(withTriples, "triples"),
              expanded = Table.ExpandRecordColumn(rows, "triples", {"name", "id", "synonym"}),
              expandSynonym = (value as any) as list =>
                let
                  token = Text.Trim(Text.Lower(ToText(value))),
                  variants =
                    List.Distinct(
                      List.Select(
                        {
                          token,
                          Text.Replace(token, "pde", ""),
                          Text.Replace(token, "pld", "")
                        },
                        each _ <> ""
                      )
                    )
                in
                  variants,
              withTokens = Table.AddColumn(
                expanded,
                "tokens",
                each
                  let
                    parts = List.Transform(Text.Split(ToText([synonym]), ":"), each Text.Trim(_)),
                    nonEmpty = List.Select(parts, each _ <> ""),
                    variants = List.Distinct(List.Combine(List.Transform(nonEmpty, each expandSynonym(_))))
                  in
                    variants,
                type list
              ),
              namesOnly = SelectColumns(withTokens, {"id", "uniprot_id_primary", "target_chembl_id", "name"}),
              cleanedNames = Table.SelectRows(
                Table.TransformColumns(namesOnly, {{"name", each Text.Trim(ToText(_)), type text}}),
                each [name] <> "" and [name] <> "n/a" and [name] <> "none"
              ),
              synonyms = SelectColumns(withTokens, {"id", "uniprot_id_primary", "target_chembl_id", "tokens"}),
              expandedTokens = Table.ExpandListColumn(synonyms, "tokens"),
              cleanedTokens = Table.RenameColumns(
                Table.SelectRows(
                  Table.TransformColumns(expandedTokens, {{"tokens", each Text.Trim(ToText(_)), type text}}),
                  each [tokens] <> "" and [tokens] <> "n/a" and [tokens] <> "none"
                ),
                {{"tokens", "name"}}
              ),
              combined = Table.Combine({cleanedNames, cleanedTokens}),
              deduplicated = Table.Distinct(combined, {"id", "name", "target_chembl_id", "uniprot_id_primary"}),
              sorted = Table.Sort(deduplicated, {{"uniprot_id_primary", Order.Ascending}, {"id", Order.Ascending}})
            in
              Table.Distinct(sorted, {"id", "name"})
        ],
      CrossrefModule =
        [
          Build = (source as table) as table =>
            let
              dropColumns = {
                "isoform_ids",
                "isoform_names",
                "isoform_synonyms",
                "organism",
                "taxon_id",
                "lineage_superkingdom",
                "lineage_phylum",
                "lineage_class",
                "xref_chembl",
                "gtop_synonyms",
                "gtop_natural_ligands_n",
                "gtop_interactions_n",
                "gtop_function_text_short",
                "uniProtkbId",
                "hgnc_name",
                "secondaryAccessions",
                "transmembrane",
                "intramembrane",
                "hgnc_id",
                "features_signal_peptide",
                "tax_id",
                "species_group_flag",
                "family",
                "xref_uniprot",
                "gene_symbol",
                "protein_name_canonical",
                "protein_name_alt",
                "sequence_length",
                "features_transmembrane",
                "features_topology",
                "ptm_glycosylation",
                "ptm_lipidation",
                "ptm_disulfide_bond",
                "ptm_modified_residue",
                "uniprot_last_update",
                "uniprot_version",
                "pipeline_version",
                "timestamp_utc",
                "recommendedName",
                "geneName",
                "molecular_function",
                "cellular_component",
                "subcellular_location",
                "topology",
                "glycosylation",
                "lipidation",
                "disulfide_bond",
                "modified_residue",
                "phosphorylation",
                "acetylation",
                "ubiquitination",
                "signal_peptide",
                "propeptide",
                "gene_symbol_list",
                "protein_synonym_list",
                "reactions",
                "reaction_ec_numbers",
                "protein_class_pred_L1",
                "protein_class_pred_L2",
                "protein_class_pred_L3",
                "protein_class_pred_rule_id",
                "protein_class_pred_evidence",
                "protein_class_pred_confidence",
                "iuphar_target_id",
                "iuphar_family_id",
                "iuphar_type",
                "iuphar_class",
                "iuphar_subclass",
                "iuphar_chain",
                "iuphar_name",
                "iuphar_full_id_path",
                "iuphar_full_name_path",
                "protein_classifications",
                "pref_name",
                "target_type",
                "target_components",
                "target_chembl_id",
                "pfam",
                "interpro"
              },
              trimmed = Drop(source, dropColumns)
            in
              trimmed
        ],
      ActivityModule =
        [
          Build = (source as table) as table =>
            let
              dropColumns = {
                "isoform_ids",
                "isoform_names",
                "isoform_synonyms",
                "organism",
                "taxon_id",
                "lineage_superkingdom",
                "lineage_phylum",
                "lineage_class",
                "xref_chembl",
                "gtop_synonyms",
                "gtop_natural_ligands_n",
                "gtop_interactions_n",
                "gtop_function_text_short",
                "uniProtkbId",
                "hgnc_name",
                "secondaryAccessions",
                "transmembrane",
                "intramembrane",
                "hgnc_id",
                "features_signal_peptide",
                "tax_id",
                "species_group_flag",
                "family",
                "xref_uniprot",
                "xref_ensembl",
                "pfam",
                "interpro",
                "xref_pdb",
                "xref_alphafold",
                "SUPFAM",
                "PROSITE",
                "InterPro",
                "Pfam",
                "PRINTS",
                "TCDB",
                "target_type",
                "protein_classifications",
                "protein_name_alt",
                "recommendedName",
                "pref_name",
                "target_components",
                "cross_references",
                "gene_symbol_list",
                "protein_synonym_list",
                "ptm_glycosylation",
                "ptm_lipidation",
                "ptm_disulfide_bond",
                "ptm_modified_residue",
                "glycosylation",
                "lipidation",
                "disulfide_bond",
                "modified_residue",
                "phosphorylation",
                "acetylation",
                "ubiquitination",
                "signal_peptide",
                "propeptide",
                "reactions"
              },
              trimmed = Drop(source, dropColumns),
              filtered = Table.SelectRows(trimmed, each ToText([reaction_ec_numbers]) <> "")
            in
              filtered
        ],
      MetadataModule =
        [
          Build = (source as table) as table =>
            let
              dropColumns = {
                "isoform_ids",
                "isoform_names",
                "isoform_synonyms",
                "organism",
                "taxon_id",
                "lineage_superkingdom",
                "lineage_phylum",
                "lineage_class",
                "xref_chembl",
                "gtop_synonyms",
                "gtop_natural_ligands_n",
                "gtop_interactions_n",
                "gtop_function_text_short",
                "uniProtkbId",
                "hgnc_name",
                "secondaryAccessions",
                "transmembrane",
                "intramembrane",
                "hgnc_id",
                "features_signal_peptide",
                "tax_id",
                "species_group_flag",
                "family",
                "xref_uniprot",
                "xref_ensembl",
                "pfam",
                "interpro",
                "xref_pdb",
                "xref_alphafold",
                "SUPFAM",
                "PROSITE",
                "InterPro",
                "Pfam",
                "PRINTS",
                "TCDB",
                "target_type",
                "protein_classifications",
                "protein_name_alt",
                "recommendedName",
                "pref_name",
                "target_components",
                "cross_references",
                "gene_symbol_list",
                "protein_synonym_list",
                "ptm_glycosylation",
                "ptm_lipidation",
                "ptm_disulfide_bond",
                "ptm_modified_residue",
                "glycosylation",
                "lipidation",
                "disulfide_bond",
                "modified_residue",
                "phosphorylation",
                "acetylation",
                "ubiquitination",
                "signal_peptide",
                "propeptide",
                "reactions",
                "reaction_ec_numbers"
              },
              trimmed = Drop(source, dropColumns),
              keepOrder = {
                "target_chembl_id",
                "uniprot_id_primary",
                "secondaryAccessions",
                "gene_symbol",
                "protein_name_canonical",
                "sequence_length",
                "features_transmembrane",
                "features_topology",
                "xref_iuphar",
                "gtop_target_id",
                "uniprot_last_update",
                "uniprot_version",
                "pipeline_version",
                "timestamp_utc",
                "geneName",
                "molecular_function",
                "cellular_component",
                "subcellular_location",
                "topology",
                "GuidetoPHARMACOLOGY",
                "protein_class_pred_L1",
                "protein_class_pred_L2",
                "protein_class_pred_L3",
                "protein_class_pred_rule_id",
                "protein_class_pred_evidence",
                "protein_class_pred_confidence",
                "iuphar_target_id",
                "iuphar_family_id",
                "iuphar_type",
                "iuphar_class",
                "iuphar_subclass",
                "iuphar_chain",
                "iuphar_name",
                "iuphar_full_id_path",
                "iuphar_full_name_path"
              },
              ordered = SelectInOrder(trimmed, keepOrder),
              sanitized = Drop(
                ordered,
                {
                  "secondaryAccessions",
                  "gene_symbol",
                  "protein_name_canonical",
                  "sequence_length",
                  "features_transmembrane",
                  "features_topology",
                  "xref_iuphar",
                  "gtop_target_id",
                  "geneName",
                  "molecular_function",
                  "cellular_component",
                  "subcellular_location",
                  "topology",
                  "GuidetoPHARMACOLOGY",
                  "protein_class_pred_L1",
                  "protein_class_pred_L2",
                  "protein_class_pred_L3",
                  "protein_class_pred_rule_id",
                  "protein_class_pred_evidence",
                  "protein_class_pred_confidence",
                  "iuphar_target_id",
                  "iuphar_family_id",
                  "iuphar_type",
                  "iuphar_class",
                  "iuphar_subclass",
                  "iuphar_chain",
                  "iuphar_name",
                  "iuphar_full_id_path",
                  "iuphar_full_name_path"
                }
              )
            in
              sanitized
        ],
      NameModule =
        [
          Build = (source as table) as table =>
            let
              dropColumns = {
                "isoform_ids",
                "isoform_names",
                "isoform_synonyms",
                "organism",
                "taxon_id",
                "lineage_superkingdom",
                "lineage_phylum",
                "lineage_class",
                "xref_chembl",
                "gtop_synonyms",
                "gtop_natural_ligands_n",
                "gtop_interactions_n",
                "gtop_function_text_short",
                "uniProtkbId",
                "hgnc_name",
                "secondaryAccessions",
                "transmembrane",
                "intramembrane",
                "hgnc_id",
                "features_signal_peptide",
                "tax_id",
                "species_group_flag",
                "family",
                "xref_uniprot",
                "xref_ensembl",
                "pfam",
                "interpro",
                "xref_pdb",
                "xref_alphafold",
                "SUPFAM",
                "PROSITE",
                "InterPro",
                "Pfam",
                "PRINTS",
                "TCDB",
                "target_type",
                "protein_classifications",
                "protein_class_pred_L1",
                "protein_class_pred_L2",
                "protein_class_pred_L3",
                "protein_class_pred_rule_id",
                "protein_class_pred_evidence",
                "protein_class_pred_confidence",
                "iuphar_target_id",
                "iuphar_family_id",
                "iuphar_type",
                "iuphar_class",
                "iuphar_subclass",
                "iuphar_chain",
                "iuphar_name",
                "iuphar_full_id_path",
                "iuphar_full_name_path",
                "protein_classifications",
                "cross_references",
                "reactions",
                "reaction_ec_numbers"
              },
              base = Drop(source, dropColumns),
              keep = {
                "target_chembl_id",
                "uniprot_id_primary",
                "recommendedName",
                "pref_name",
                "protein_name_canonical",
                "protein_name_alt",
                "geneName",
                "gene_symbol_list",
                "secondaryAccessions",
                "target_components"
              },
              ordered = SelectInOrder(base, keep),
              normalizedAlt = Table.TransformColumns(
                ordered,
                {
                  {"gene_symbol_list", each Text.Lower(Text.Replace(Text.Replace(ToText(_), "[", ""), "]", "")), type text},
                  {"protein_name_alt", each let s = Text.Replace(Text.Replace(ToText(_), "[", ""), "]", "") in if s = "[]" or s = "" then null else s, type nullable text}
                }
              ),
              components1 = try
                Table.SplitColumn(
                  normalizedAlt,
                  "target_components",
                  Splitter.SplitTextByEachDelimiter({"""component_description"": """}, QuoteStyle.None, false),
                  {"tc1", "tc2"}
                )
              otherwise
                normalizedAlt,
              components2 = try
                Table.SplitColumn(
                  components1,
                  "tc2",
                  Splitter.SplitTextByEachDelimiter({""", """}, QuoteStyle.None, false),
                  {"tc2a", "tc2b"}
                )
              otherwise
                components1,
              trimmedComponents = DropColumns(components2, {"tc1", "tc2b"}),
              reordered = Table.ReorderColumns(
                trimmedComponents,
                List.Intersect(
                  {
                    {
                      "uniprot_id_primary",
                      "protein_name_alt",
                      "tc2a",
                      "recommendedName",
                      "pref_name",
                      "protein_name_canonical",
                      "geneName",
                      "gene_symbol_list"
                    },
                    Table.ColumnNames(trimmedComponents)
                  }
                ),
                MissingField.Ignore
              ),
              cleaned = Table.TransformColumns(
                reordered,
                {
                  {"geneName", Text.Lower, type nullable text},
                  {"gene_symbol_list", Text.Lower, type text},
                  {"protein_name_canonical", ToText, type text},
                  {"pref_name", ToText, type text}
                }
              ),
              synonyms = Table.AddColumn(
                cleaned,
                "synonyms",
                each
                  let
                    parts = List.RemoveNulls({[protein_name_canonical], [pref_name], [tc2a], [protein_name_alt], [gene_symbol_list]})
                  in
                    Text.Combine(List.Distinct(List.Combine(List.Transform(parts, SplitPipe))), "|"),
                type text
              ),
              renamed = Table.RenameColumns(
                DropColumns(synonyms, {"tc2a"}),
                {{"recommendedName", "recommended_name"}, {"geneName", "gene_name"}},
                MissingField.Ignore
              ),
              selected = SelectInOrder(
                renamed,
                {
                  "target_chembl_id",
                  "uniprot_id_primary",
                  "recommended_name",
                  "gene_name",
                  "synonyms"
                }
              )
            in
              selected
        ],
      OrganismModule =
        [
          Build = (source as table) as table =>
            let
              base = SelectColumns(
                source,
                {
                  "target_chembl_id",
                  "uniprot_id_primary",
                  "organism",
                  "taxon_id",
                  "lineage_superkingdom",
                  "lineage_phylum",
                  "lineage_class",
                  "reaction_ec_numbers"
                }
              ),
              lowered = Table.TransformColumns(
                base,
                {
                  {"lineage_superkingdom", Text.Lower, type text},
                  {"lineage_phylum", Text.Lower, type text},
                  {"lineage_class", Text.Lower, type text}
                }
              ),
              withCellularity = Cellularity_[AddCellularitySmart](lowered, "taxon_id", "lineage_superkingdom", "lineage_phylum"),
              withEc = Table.AddColumn(
                withCellularity,
                "ec_major_list",
                each
                  let
                    items = SplitPipe([reaction_ec_numbers])
                  in
                    List.Distinct(
                      List.Transform(
                        items,
                        each if Text.Contains(_, ".") then Text.Split(_, "."){0} else _
                      )
                    ),
                type list
              ),
              withFlag = Table.AddColumn(
                withEc,
                "multifunctional_enzyme",
                each List.Count([ec_major_list]) > 1,
                type logical
              ),
              final = Table.RemoveColumns(withFlag, {"ec_major_list"}, MissingField.Ignore)
            in
              final
        ],
      MainModule =
        let
          dropColumns = {
            "isoform_ids",
            "isoform_names",
            "isoform_synonyms",
            "organism",
            "taxon_id",
            "lineage_superkingdom",
            "lineage_phylum",
            "lineage_class",
            "xref_chembl",
            "gtop_synonyms",
            "gtop_natural_ligands_n",
            "gtop_interactions_n",
            "gtop_function_text_short",
            "uniProtkbId",
            "hgnc_name",
            "secondaryAccessions",
            "transmembrane",
            "intramembrane",
            "hgnc_id",
            "features_signal_peptide",
            "tax_id",
            "species_group_flag",
            "family",
            "xref_uniprot",
            "xref_ensembl",
            "pfam",
            "interpro",
            "xref_pdb",
            "xref_alphafold",
            "SUPFAM",
            "PROSITE",
            "InterPro",
            "Pfam",
            "PRINTS",
            "TCDB",
            "target_type",
            "protein_classifications",
            "protein_name_alt",
            "recommendedName",
            "pref_name",
            "target_components",
            "cross_references",
            "gene_symbol_list",
            "protein_synonym_list",
            "ptm_glycosylation",
            "ptm_lipidation",
            "ptm_disulfide_bond",
            "ptm_modified_residue",
            "glycosylation",
            "lipidation",
            "disulfide_bond",
            "modified_residue",
            "phosphorylation",
            "acetylation",
            "ubiquitination",
            "signal_peptide",
            "propeptide",
            "reactions",
            "reaction_ec_numbers"
          },
          keepOrder = {
            "target_chembl_id",
            "uniprot_id_primary",
            "secondaryAccessions",
            "gene_symbol",
            "protein_name_canonical",
            "sequence_length",
            "features_transmembrane",
            "features_topology",
            "xref_iuphar",
            "gtop_target_id",
            "uniprot_last_update",
            "uniprot_version",
            "pipeline_version",
            "timestamp_utc",
            "geneName",
            "molecular_function",
            "cellular_component",
            "subcellular_location",
            "topology",
            "GuidetoPHARMACOLOGY",
            "protein_class_pred_L1",
            "protein_class_pred_L2",
            "protein_class_pred_L3",
            "protein_class_pred_rule_id",
            "protein_class_pred_evidence",
            "protein_class_pred_confidence",
            "iuphar_target_id",
            "iuphar_family_id",
            "iuphar_type",
            "iuphar_class",
            "iuphar_subclass",
            "iuphar_chain",
            "iuphar_name",
            "iuphar_full_id_path",
            "iuphar_full_name_path"
          },
          sanitizeAfter = {
            "uniprot_last_update",
            "uniprot_version",
            "pipeline_version",
            "timestamp_utc",
            "geneName",
            "xref_iuphar",
            "gtop_target_id",
            "GuidetoPHARMACOLOGY",
            "sequence_length",
            "features_transmembrane",
            "features_topology",
            "molecular_function",
            "cellular_component",
            "subcellular_location",
            "topology"
          }
        in
          [
            Build = (source as table) as table =>
              let
                trimmed = Drop(source, dropColumns),
                ordered = SelectInOrder(trimmed, keepOrder),
                sanitized = Drop(ordered, sanitizeAfter),
                names = NameModule[Build](source),
                joined = JoinExpand(
                  sanitized,
                  {"target_chembl_id", "uniprot_id_primary"},
                  names,
                  {"target_chembl_id", "uniprot_id_primary"},
                  "TargetNames",
                  {"recommended_name", "gene_name", "synonyms"},
                  {"recommended_name", "gene_name", "synonyms"}
                ),
                normalized = Table.TransformColumns(joined, {{"gene_name", Text.Lower, type text}}),
                final = Drop(normalized, {"protein_name_canonical", "gene_symbol"})
              in
                final
          ],
      PtmModule =
        [
          ParseAAList = (src as table, columnName as text, mapToken as function, optional keepPos as nullable logical) as table =>
            let
              cleaned = Drop(src, {}),
              values = cleaned[columnName],
              rawTable = Table.FromList(values, Splitter.SplitByNothing(), {"raw"}),
              withIndex = Table.AddIndexColumn(rawTable, "Index", 0, 1),
              filtered = Table.SelectRows(withIndex, each [raw] <> null and Text.Trim(ToText([raw])) <> ""),
              tokenized = Table.TransformColumns(filtered, {{"raw", each List.Select(SplitPipe(_), each _ <> ""), type list}}),
              expanded = Table.ExpandListColumn(tokenized, "raw"),
              splitTokens = Table.SplitColumn(expanded, "raw", Splitter.SplitTextByEachDelimiter({"["}, null, false), {"token", "posRaw"}),
              normalized = Table.TransformColumns(
                splitTokens,
                {
                  {"token", each Text.Lower(Text.Trim(ToText(_))), type text},
                  {"posRaw", each Text.Trim(Text.Replace(ToText(_), "]", "")), type text}
                }
              ),
              mapped = Table.TransformColumns(normalized, {{"token", each mapToken(_), type text}}),
              prepared =
                if keepPos = null or keepPos = true then
                  Table.AddColumn(mapped, "AA", each [token] & [posRaw], type text)
                else
                  Table.RenameColumns(mapped, {{"token", "AA"}}),
              grouped = Table.Group(
                prepared,
                {"Index"},
                {{"Count", each Table.RowCount(_), Int64.Type}, {"aa_list", each Text.Combine(List.RemoveNulls([AA]), "|"), type text}}
              )
            in
              grouped,
          MapPhospho = (s as text) as text => if s = "phosphoserine" then "pS" else if s = "phosphothreonine" then "pT" else if s = "phosphotyrosine" then "pY" else s,
          MapAA1 = (s as text) as text =>
            let
              replacements = [
                valine = "V",
                alanine = "A",
                threonine = "T",
                serine = "S",
                lysine = "K",
                tyrosine = "Y",
                arginine = "R",
                tryptophan = "W",
                methionine = "M",
                glycine = "G",
                cysteine = "C",
                glutamate = "E",
                glutamic_acid = "E",
                aspartate = "D",
                aspartic_acid = "D",
                histidine = "H",
                asparagine = "N",
                glutamine = "Q",
                proline = "P"
              ],
              key = Text.Lower(Text.Trim(s))
            in
              if Record.HasFields(replacements, key) then Record.Field(replacements, key) else key
        ],
      IupharModule =
        let
          normalizeTable19 = () as table =>
            let
              rows = table19(),
              typed = TransformColumnTypesSafe(
                rows,
                {
                  {"ec", Int64.Type},
                  {"iuphar_target_id", type text},
                  {"iuphar_family_id", type text},
                  {"iuphar_type", type text},
                  {"iuphar_class", type text},
                  {"iuphar_subclass", type text},
                  {"iuphar_chain", type text},
                  {"iuphar_full_id_path", type text},
                  {"iuphar_full_name_path", type text},
                  {"Column1", type text},
                  {"Column2", type text}
                }
              ),
              cleaned1 = Table.ReplaceValue(typed, "Enzyme: Protease", "Enzyme", Replacer.ReplaceText, {"Column1"}),
              cleaned2 = Table.ReplaceValue(cleaned1, "Enzyme: Kinase", "Enzyme", Replacer.ReplaceText, {"Column1"}),
              cleaned3 = Table.ReplaceValue(cleaned2, "Ion Channel: Voltage-gated", "Voltage-gated", Replacer.ReplaceText, {"Column2"}),
              cleaned4 = Table.ReplaceValue(cleaned3, "Ion Channel: Ligand-gated", "Ligand-gated", Replacer.ReplaceText, {"Column2"})
            in
              Table.SelectRows(cleaned4, each [Column1] <> null),
          classification = Table.Buffer(normalizeTable19()),
          dropPrediction = PredCols,
          dropIupharId = IupharIdCols,
          dropForKnown = List.Combine({dropPrediction, {"ec_number", "gene_name", "synonyms", "organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme", "known_id"}}),
          mergeIuphar = (src as table) as table =>
            let
              trimmed = Drop(src, List.Combine({dropPrediction, dropIupharId})),
              joined = Table.NestedJoin(trimmed, {"target_chembl_id"}, buildIntermediate(), {"target_chembl_id"}, "IUPHAR", JoinKind.LeftOuter),
              expanded = ExpandTableColumnSafe(
                joined,
                "IUPHAR",
                List.Combine({dropPrediction, dropIupharId}),
                List.Combine({dropPrediction, dropIupharId})
              )
            in
              expanded,
          buildIntermediate = () as table =>
            IUPHAR_int(),
          buildKnown = () as table =>
            let
              mainBase = MainModule[Build](Data[Target_out]),
              organismBase = OrganismModule[Build](Data[Target_out]),
              activityBase = ActivityModule[Build](Data[Target_out]),
              joinedOrg = Table.NestedJoin(
                mainBase,
                {"target_chembl_id"},
                organismBase,
                {"target_chembl_id"},
                "org",
                JoinKind.LeftOuter
              ),
              expandedOrg = ExpandTableColumnSafe(
                joinedOrg,
                "org",
                {"organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme"},
                {"organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme"}
              ),
              joinedAct = Table.NestedJoin(
                expandedOrg,
                {"target_chembl_id", "uniprot_id_primary"},
                activityBase,
                {"target_chembl_id", "uniprot_id_primary"},
                "act",
                JoinKind.LeftOuter
              ),
              expandedAct = ExpandTableColumnSafe(joinedAct, "act", {"reaction_ec_numbers"}, {"ec_number"}),
              direct = mergeIuphar(expandedAct),
              baseKnown = Table.SelectRows(direct, each ToText([iuphar_family_id]) <> "N/A" or ToText([cellularity]) = "unicellular"),
              trimmedBase = Drop(baseKnown, {"gene_name", "synonyms", "organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme", "ec_number"}),
              fallback =
                let
                  candidates = Table.SelectRows(direct, each ToText([iuphar_full_id_path]) = ""),
                  trimmedCandidates = Drop(candidates, dropIupharId),
                  joinClass1 = Table.NestedJoin(trimmedCandidates, {"protein_class_pred_L1"}, classification, {"iuphar_class"}, "cls", JoinKind.LeftOuter),
                  expandClass1 = ExpandTableColumnSafe(
                    joinClass1,
                    "cls",
                    List.Combine({dropIupharId, {"Column1", "Column2"}}),
                    List.Combine({dropIupharId, {"Column1", "Column2"}})
                  ),
                  filteredClass1 = Table.SelectRows(expandClass1, each [Column2] <> null),
                  cleanedClass1 = Drop(filteredClass1, {"Column1", "Column2"})
                in
                  cleanedClass1,
              fallback2 =
                let
                  candidates = Table.SelectRows(direct, each ToText([iuphar_full_id_path]) = ""),
                  trimmedCandidates = Drop(candidates, dropIupharId),
                  joinClass2 = Table.NestedJoin(trimmedCandidates, {"protein_class_pred_L2"}, classification, {"Column2"}, "cls", JoinKind.LeftOuter),
                  expandClass2 = ExpandTableColumnSafe(
                    joinClass2,
                    "cls",
                    List.Combine({dropIupharId, {"Column1", "Column2"}}),
                    List.Combine({dropIupharId, {"Column1", "Column2"}})
                  ),
                  filteredClass2 = Table.SelectRows(expandClass2, each [iuphar_chain] <> null),
                  dedupClass2 = Table.Distinct(filteredClass2, {"target_chembl_id", "uniprot_id_primary"})
                in
                  Table.Distinct(dedupClass2, {"target_chembl_id"}),
              enzymeFallback =
                let
                  candidates = Table.SelectRows(direct, each ToText([iuphar_family_id]) = "N/A" and ToText([cellularity]) = "multicellular" and [ec_number] <> null),
                  splitted = Table.SplitColumn(candidates, "ec_number", Splitter.SplitTextByEachDelimiter({"."}, null, false), {"ec_major", "ec_minor"}),
                  filteredMajor = Table.SelectRows(splitted, each [ec_major] <> "3"),
                  typed = TransformColumnTypesSafe(filteredMajor, {{"ec_major", Int64.Type}, {"ec_minor", type text}}),
                  joined = Table.NestedJoin(typed, {"ec_major"}, classification, {"ec"}, "cls", JoinKind.LeftOuter),
                  trimmed = Drop(joined, dropIupharId),
                  expanded = ExpandTableColumnSafe(trimmed, "cls", dropIupharId, dropIupharId),
                  appended = Table.Combine({expanded, fallback, fallback2}),
                  deduplicated = Table.Distinct(appended, {"target_chembl_id"}),
                  cleaned = Drop(
                    deduplicated,
                    {
                      "secondaryAccessions",
                      "gene_name",
                      "synonyms",
                      "organism",
                      "taxon_id",
                      "lineage_superkingdom",
                      "lineage_phylum",
                      "lineage_class",
                      "cellularity",
                      "multifunctional_enzyme",
                      "ec_major",
                      "ec_minor",
                      "Column1",
                      "Column2"
                    }
                  )
                in
                  cleaned,
              knownUnion = Table.Combine({trimmedBase, enzymeFallback}),
              distinctKnown = Table.Distinct(knownUnion, {"target_chembl_id"})
            in
              distinctKnown,
          buildFinal = () as table =>
            let
              activityBase = ActivityModule[Build](Data[Target_out]),
              knownTable = buildKnown(),
              distinctKnown = Table.Distinct(knownTable, {"target_chembl_id"}),
              joined = Table.NestedJoin(
                activityBase,
                {"target_chembl_id"},
                distinctKnown,
                {"target_chembl_id"},
                "Known",
                JoinKind.LeftOuter
              ),
              expanded = ExpandTableColumnSafe(joined, "Known", {"target_chembl_id"}, {"known_id"}),
              withoutKnown = Table.SelectRows(expanded, each [known_id] = null),
              distinctTargets = Table.Distinct(withoutKnown, {"target_chembl_id"}),
              withFallbackKey = Table.AddColumn(distinctTargets, "ec_key", each 100, Int64.Type),
              joinedClass = Table.NestedJoin(withFallbackKey, {"ec_key"}, classification, {"ec"}, "cls", JoinKind.LeftOuter),
              trimmed = Drop(joinedClass, dropIupharId),
              expandedClass = ExpandTableColumnSafe(trimmed, "cls", dropIupharId, dropIupharId),
              withoutTech = Drop(expandedClass, {"ec_number", "ec_key"}),
              combined = Table.Combine({distinctKnown, withoutTech}),
              deduplicated = Table.Distinct(combined, {"target_chembl_id"})
            in
              Drop(deduplicated, dropForKnown)
        in
          [
            Build = buildFinal,
            BuildKnown = buildKnown,
            BuildIntermediate = buildIntermediate
          ]
    in
      [
        Isoform = IsoformModule,
        Crossref = CrossrefModule,
        Activity = ActivityModule,
        Metadata = MetadataModule,
        Name = NameModule,
        Organism = OrganismModule,
        Main = MainModule,
        PTM = PtmModule,
        IUPHAR = IupharModule
      ],
  TargetHandlersType =
    type [
      isoform = function (source as table) as table,
      crossref = function (source as table) as table,
      activity = function (source as table) as table,
      metadata = function (source as table) as table,
      name = function (source as table) as table,
      organism = function (source as table) as table,
      main = function (source as table) as table,
      PTM = record,
      IUPHAR = function () as table,
      known = function () as table
    ],
  TargetHandlers =
    Value.ReplaceType(
      [
        isoform = TargetModules[Isoform][Build],
        crossref = TargetModules[Crossref][Build],
        activity = TargetModules[Activity][Build],
        metadata = TargetModules[Metadata][Build],
        name = TargetModules[Name][Build],
        organism = TargetModules[Organism][Build],
        main = TargetModules[Main][Build],
        PTM = TargetModules[PTM],
        IUPHAR = () as table => TargetModules[IUPHAR][Build](),
        known = () as table => TargetModules[IUPHAR][BuildKnown]()
      ],
      TargetHandlersType

    ),

  // ===== get_target =====
  // Назначение: объединение таргетных атрибутов и справочников IUPHAR.
  get_target = () =>
    let
      classificationColumns = {
        "protein_class_pred_L1",
        "protein_class_pred_L2",
        "protein_class_pred_L3",
        "protein_class_pred_rule_id",
        "protein_class_pred_evidence",
        "protein_class_pred_confidence",
        "protein_classifications",
        "iuphar_target_id",
        "iuphar_family_id",
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "iuphar_chain",
        "iuphar_name",
        "iuphar_full_id_path",
        "iuphar_full_name_path"
      },
      organismAttributes = {
        "taxon_id",
        "lineage_superkingdom",
        "lineage_phylum",
        "lineage_class",
        "reaction_ec_numbers",
        "cellularity",
        "multifunctional_enzyme"
      },
      iupharAttributes = {
        "iuphar_name",
        "iuphar_target_id",
        "iuphar_family_id",
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "iuphar_chain",
        "iuphar_full_id_path",
        "iuphar_full_name_path"
      },
      outputColumns = {
        "target_chembl_id",
        "uniprot_id_primary",
        "recommended_name",
        "gene_name",
        "synonyms",
        "protein_class_pred_L1",
        "protein_class_pred_L2",
        "protein_class_pred_L3",
        "protein_class_pred_rule_id",
        "protein_class_pred_evidence",
        "protein_class_pred_confidence",
        "taxon_id",
        "lineage_superkingdom",
        "lineage_phylum",
        "lineage_class",
        "reaction_ec_numbers",
        "cellularity",
        "multifunctional_enzyme",
        "iuphar_name",
        "iuphar_target_id",
        "iuphar_family_id",
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
        "iuphar_chain",
        "iuphar_full_id_path",
        "iuphar_full_name_path"
      },
      baseMain = TargetHandlers[main](Data[Target_out]),
      sanitizedMain = DropColumns(baseMain, classificationColumns),
      organismSource = TargetHandlers[organism](Data[Target_out]),
      organismTable = Table.Buffer(
        SelectColumnsSafe(
          organismSource,
          List.Combine({{"target_chembl_id"}, organismAttributes})
        )
      ),
      withOrganism = JoinAndExpand(
        sanitizedMain,
        {"target_chembl_id"},
        organismTable,
        {"target_chembl_id"},
        "Organism",
        organismAttributes,
        organismAttributes
      ),
      iupharTable = Table.Buffer(
        SelectColumnsSafe(
          TargetHandlers[IUPHAR](),
          List.Combine({{"target_chembl_id"}, iupharAttributes})
        )
      ),
      withIuphar = JoinAndExpand(
        withOrganism,
        {"target_chembl_id"},
        iupharTable,
        {"target_chembl_id"},
        "IUPHAR",
        iupharAttributes,
        iupharAttributes
      ),
      ordered = SelectColumnsInOrder(withIuphar, outputColumns)
    in
      ordered,
  data_validation = Record.AddField(data_validation_base, "get_target", get_target),
  // ===== Exports =====
  // Назначение: единая точка доступа к публичным функциям модуля.
  Exports =
    [
      get_validation = data_validation[get_validation],
      get_document = data_validation[get_document],
      get_testitem = data_validation[get_testitem],
      get_assay = data_validation[get_assay],
      get_target = data_validation[get_target],
      get_target_handlers = () as record => TargetHandlers
    ],
  Result =
    [
      Exports = Exports,
      _document = Exports[get_document],
      _testitem = Exports[get_testitem],
      _assay = Exports[get_assay],
      _target = Exports[get_target],
      _validation = Exports[get_validation],
      _target_handlers = Exports[get_target_handlers],
      TargetHandlers = Exports[get_target_handlers](),
      ParamsSummary = ParamsSummary
    ]
in
  Result
