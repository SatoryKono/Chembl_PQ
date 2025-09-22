// ===== Changelog =====


let
  // ===== Parameters =====
  // Paths: relative file locations. Encodings: named code pages. Delimiters: reusable separators. PathEncodingKeys: map path keys to encoding keys. Defaults: fallback keys for loaders. Tables: logical dataset handles for inputs/outputs.
  Parameters = [
    BasePath = "E:\github\ChEMBL_dataAcquisition\",    // пример: "E:\github\ChEMBL_dataAcquisition\"
    SourceKind = "File",  // "File" | "SharePoint" | "Http"
    SharePointSiteUrl = "", // TODO: указать URL сайта SharePoint при SourceKind = "SharePoint"
    Defaults = [
      EncodingKey = "Utf8",
      DelimiterKey = "Csv"
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
  GetSourceContent = (absPath as text) as binary =>
    let
      sourceKind = Parameters[SourceKind],
      content =
        if sourceKind = "File" then
          File.Contents(absPath)
        else if sourceKind = "Http" then
          Web.Contents(absPath)
        else if sourceKind = "SharePoint" then
          let
            _site = Parameters[SharePointSiteUrl],
            _todo = SharePoint.Contents(_site) // TODO: навигация к файлу и выбор нужного элемента
          in
            error "TODO: реализовать загрузку файла через SharePoint.Contents"
        else
          error "Unknown SourceKind"
    in
      content,
  CheckPathExists = (pathRel as text) as logical =>
    let
      absPath = BuildAbsolutePath(pathRel),
      sourceKind = Parameters[SourceKind],
      exists =
        if sourceKind = "File" then
          let
            probe = try File.Contents(absPath) otherwise null
          in
            probe <> null
        else
          true
    in
      exists,

  // ===== Loaders =====
  ResolveEncoding = (pathKey as text) as number =>
    let
      defaultEncodingKey = Parameters[Defaults][EncodingKey],
      encodingKey = Record.FieldOrDefault(Parameters[PathEncodingKeys], pathKey, defaultEncodingKey),
      encodingValue =
        Record.FieldOrDefault(
          Parameters[Encodings],
          encodingKey,
          Record.Field(Parameters[Encodings], defaultEncodingKey)
        )
    in
      encodingValue,
  GetDelimiter = () as text =>
    let
      delimiterKey = Parameters[Defaults][DelimiterKey]
    in
      Record.Field(Parameters[Delimiters], delimiterKey),
  LoadCsv = (pathRel as text, optional encoding as nullable number) as table =>
    let
      absPath = BuildAbsolutePath(pathRel),
      defaultEncoding = Record.Field(Parameters[Encodings], Parameters[Defaults][EncodingKey]),
      effectiveEncoding = if encoding <> null then encoding else defaultEncoding,
      content = GetSourceContent(absPath),
      csv = Csv.Document(content, [Delimiter = GetDelimiter(), Encoding = effectiveEncoding]),
      tbl = Table.PromoteHeaders(csv)
    in
      tbl,
  LoadCsvByKey = (pathKey as text) as table =>
    let
      relPath = Record.Field(Parameters[Paths], pathKey),
      encoding = ResolveEncoding(pathKey)
    in
      LoadCsv(relPath, encoding),
  Data = [
    Document_in = LoadCsvByKey("document_csv"),
    Activity_in = LoadCsvByKey("activity_csv"),
    Testitem_in = LoadCsvByKey("testitem_csv"),

    Target_out = LoadCsvByKey("target_csv_out"),
    Document_out = LoadCsvByKey("document_csv_out"),
    Assay_out = LoadCsvByKey("assay_csv_out"),
    Testitem_out = LoadCsvByKey("testitem_csv_out")
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
      transformColumnTypesSafe = (tbl as table, typeList as list, optional culture as nullable text) as table =>
        let
          columnNames = Table.ColumnNames(tbl),
          existing = List.Select(typeList, each List.Contains(columnNames, _{0})),
          transformed =
            if culture <> null then
              Table.TransformColumnTypes(tbl, existing, culture)
            else
              Table.TransformColumnTypes(tbl, existing)
        in
          transformed,
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
        TransformColumnTypesSafe = transformColumnTypesSafe,
        NormalizePages = normalizePages,
        TryNumber = tryNumber,
        NormalizeDoi = normalizeDoi
      ],
  ToText = Helpers[ToText],
  NormalizeWhitespace = Helpers[NormalizeWhitespace],
  CleanPipe = Helpers[CleanPipe],
  EnsureColumn = Helpers[EnsureColumn],
  TransformColumnTypesSafe = Helpers[TransformColumnTypesSafe],
  NormalizePages = Helpers[NormalizePages],
  TryNumber = Helpers[TryNumber],
  NormalizeDoi = Helpers[NormalizeDoi],
  SelectColumnsSafe = (tbl as table, columns as list) as table =>
    Table.SelectColumns(tbl, columns, MissingField.Ignore),
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
// ===================== document_input =====================
  ReferenceThresholdsRaw = LoadCsv(Paths[citation_fraction_csv], Encodings[Latin1]),
  ReferenceThresholdsTyped = Table.TransformColumnTypes(
    ReferenceThresholdsRaw,
    {
      {"N", Int64.Type},
      {"K_min_significant", Int64.Type},
      {"test_used_at_threshold", type text},
      {"p_value_at_threshold", type number}
    }
  ),
  ReferenceThresholds = Table.Buffer(SelectColumnsSafe(ReferenceThresholdsTyped, {"N", "K_min_significant"})),
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
            t0 = LoadCsvByKey("citation_fraction_csv"),
            t1 = Table.TransformColumnTypes(
              t0,
              {
                {"N", Int64.Type},
                {"K_min_significant", Int64.Type},
                {"test_used_at_threshold", type text},
                {"p_value_at_threshold", type number}
              }
            ),
            t2 = Table.RemoveColumns(t1, List.Difference(Table.ColumnNames(t1), {"N", "K_min_significant"}), MissingField.Ignore)
          in
            t2,
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
                Table.TransformColumnTypes(#table({"document_chembl_id", "n_assay"}, {}), {{"n_assay", Int64.Type}}),
            typed = Table.TransformColumnTypes(result, {{"n_assay", Int64.Type}}, "en-US")
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
  document_input = [
    _PubMed = (SoursePubMed as table) =>
      let
        Removed = Table.RemoveColumns(
          SoursePubMed,
          {
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
          MissingField.Ignore
        ),
        Repl0 = Table.ReplaceValue(Removed, null, 0, Replacer.ReplaceValue, {"PubMed.Volume", "PubMed.Issue", "PubMed.StartPage", "PubMed.EndPage"}),
        CastTxt = Table.TransformColumnTypes(Repl0, {{"PubMed.ArticleTitle", type text}, {"PubMed.Abstract", type text}}),
        LowerAux = Table.TransformColumns(
          Table.TransformColumnTypes(
            CastTxt,
            {
              {"PubMed.YearCompleted", type text},
              {"PubMed.MonthCompleted", type text},
              {"PubMed.DayCompleted", type text},
              {"PubMed.YearRevised", type text},
              {"PubMed.MonthRevised", type text},
              {"PubMed.DayRevised", type text}
            },
            "en-US"
          ),
          {
            {"PubMed.PublicationType", Text.Lower},
            {"PubMed.MeSH_Descriptors", Text.Lower},
            {"PubMed.MeSH_Qualifiers", Text.Lower},
            {"PubMed.ChemicalList", Text.Lower},
            {"PubMed.YearCompleted", Text.Lower},
            {"PubMed.MonthCompleted", Text.Lower},
            {"PubMed.DayCompleted", Text.Lower},
            {"PubMed.YearRevised", Text.Lower},
            {"PubMed.MonthRevised", Text.Lower},
            {"PubMed.DayRevised", Text.Lower}
          }
        ),
        Fill0 = Table.ReplaceValue(
          LowerAux,
          null,
          "0",
          Replacer.ReplaceValue,
          {"PubMed.YearCompleted", "PubMed.MonthCompleted", "PubMed.DayCompleted", "PubMed.YearRevised", "PubMed.MonthRevised", "PubMed.DayRevised"}
        ),
        VolIssTxt = Table.TransformColumnTypes(
          Fill0,
          {
            {"PubMed.Volume", type text},
            {"PubMed.Issue", type text},
            {"PubMed.StartPage", type text},
            {"PubMed.EndPage", type text}
          }
        ),
        RenVI = Table.RenameColumns(VolIssTxt, {{"PubMed.Volume", "volume"}, {"PubMed.Issue", "issue"}}),
        Pages = Table.CombineColumns(RenVI, {"PubMed.StartPage", "PubMed.EndPage"}, Combiner.CombineTextByDelimiter("-", QuoteStyle.None), "pages"),
        Ren1 = Table.RenameColumns(Pages, {{"PubMed.ISSN", "ISSN"}, {"PubMed.PublicationType", "publication_type"}}),
        Ren2 = Table.RenameColumns(
          Ren1,
          {
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
          }
        ),
        Reorder = Table.ReorderColumns(
          Ren2,
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
        Lower = Table.TransformColumns(
          Reorder,
          {
            {"publication_type", Text.Lower},
            {"title", Text.Lower},
            {"abstract", Text.Lower},
            {"DOI", Text.Lower},
            {".journal", Text.Lower}
          }
        ),
        Ren3 = Table.RenameColumns(Lower, {{"pages", "page"}, {"DOI", "PubMed.doi"}})
      in
        Ren3,
    _scholar = (SourseScholar as table) =>
      let
        Removed = Table.RemoveColumns(
          SourseScholar,
          {
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
          MissingField.Ignore
        ),
        Cast = Table.TransformColumnTypes(
          Removed,
          {
            {"scholar.ExternalIds", type text},
            {"scholar.PublicationTypes", type text},
            {"scholar.Venue", type text},
            {"scholar.DOI", type text}
          }
        ),
        Repl = Table.ReplaceValue(Cast, "0", null, Replacer.ReplaceValue, {"scholar.DOI", "scholar.PublicationTypes"}),
        Lower = Table.TransformColumns(
          Repl,
          {
            {"scholar.DOI", Text.Lower},
            {"scholar.PublicationTypes", Text.Lower},
            {"scholar.Venue", Text.Lower},
            {"scholar.ExternalIds", Text.Lower}
          }
        ),
        FixErr = Table.ReplaceValue(Lower, 0, null, Replacer.ReplaceValue, {"scholar.Error"}),
        Ren = Table.RenameColumns(FixErr, {{"scholar.DOI", "scholar.doi"}})
      in
        Ren,
    _ChEMBL = (SourseChEMBL as table) =>
      let
        Removed = Table.RemoveColumns(
          SourseChEMBL,
          {
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
          MissingField.Ignore
        ),
        Cast = Table.TransformColumnTypes(Removed, {{"ChEMBL.volume", type text}, {"ChEMBL.issue", type text}, {"ChEMBL.doi", type text}}),
        Ren1 = Table.RenameColumns(Cast, {{"PubMed.PMID", "PMID"}, {"PubMed.DOI", "DOI"}}),
        Ren2 = Table.RenameColumns(Ren1, {{"ChEMBL.title", "title"}, {"ChEMBL.document_chembl_id", "document_chembl_id"}, {"ChEMBL.abstract", "abstract"}}),
        Lower = Table.TransformColumns(Ren2, {{"DOI", Text.Lower}, {"ChEMBL.doi", Text.Lower}}),
        Same = Table.AddColumn(Lower, "same_doi", each [DOI] = [ChEMBL.doi], type logical),
        Ren3 = Table.RenameColumns(
          Same,
          {
            {"ChEMBL.journal_abbrev", "journal"},
            {"ChEMBL.volume", "volume"},
            {"ChEMBL.issue", "issue"},
            {"ChEMBL.authors", "authors"},
            {"ChEMBL.year", "year"}
          }
        ),
        Repl0 = Table.ReplaceValue(Ren3, null, 0, Replacer.ReplaceValue, {"volume", "issue", "ChEMBL.first_page", "ChEMBL.last_page"}),
        Lower2 = Table.TransformColumns(
          Repl0,
          {
            {"title", Text.Lower},
            {"abstract", Text.Lower},
            {"ChEMBL.doi", Text.Lower},
            {"authors", Text.Lower},
            {"journal", Text.Lower}
          }
        ),
        Pages = Table.CombineColumns(
          Table.TransformColumnTypes(Lower2, {{"ChEMBL.first_page", type text}, {"ChEMBL.last_page", type text}}, "en-US"),
          {"ChEMBL.first_page", "ChEMBL.last_page"},
          Combiner.CombineTextByDelimiter("-", QuoteStyle.None),
          "page"
        ),
        Removed2 = Table.RemoveColumns(Pages, {"DOI"}, MissingField.Ignore)
      in
        Removed2,
    _OpenAlex = (SourseOpenAlex as table) =>
      let
        Cast = Table.TransformColumnTypes(SourseOpenAlex, {{"PubMed.PMID", Int64.Type}, {"PubMed.DOI", type text}, {"OpenAlex.DOI", type text}}),
        Removed = Table.RemoveColumns(
          Cast,
          {
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
          MissingField.Ignore
        ),
        Ren = Table.RenameColumns(Removed, {{"PubMed.PMID", "PMID"}, {"PubMed.DOI", "DOI"}}),
        Flag = Table.AddColumn(Ren, "same_doi", each [DOI] = [OpenAlex.DOI], type logical),
        Ren1 = Table.RenameColumns(
          Flag,
          {
            {"OpenAlex.PublicationTypes", "publication_type"},
            {"OpenAlex.TypeCrossref", "crossref_type"},
            {"OpenAlex.MeshDescriptors", "MeSH.descriptors"}
          }
        ),
        Removed2 = Table.RemoveColumns(Ren1, {"OpenAlex.MeshQualifiers"}, MissingField.Ignore),
        Ren2 = Table.RenameColumns(Removed2, {{"OpenAlex.Id", "id"}, {"OpenAlex.Error", "Error"}}),
        Lower = Table.TransformColumns(
          Ren2,
          {
            {"MeSH.descriptors", Text.Lower},
            {"crossref_type", Text.Lower},
            {"OpenAlex.DOI", Text.Lower},
            {"DOI", Text.Lower}
          }
        ),
        Removed3 = Table.RemoveColumns(Lower, {"DOI"}, MissingField.Ignore),
        Ren3 = Table.RenameColumns(Removed3, {{"OpenAlex.DOI", "OpenAlex.doi"}})
      in
        Ren3,
    _crossref = (Soursecrossref as table) =>
      let
        Cast = Table.TransformColumnTypes(
          Soursecrossref,
          {
            {"PubMed.PMID", Int64.Type},
            {"PubMed.DOI", type text},
            {"crossref.DOI", type text},
            {"crossref.Title", type text},
            {"crossref.Type", type text}
          }
        ),
        Ren = Table.RenameColumns(Cast, {{"PubMed.PMID", "PMID"}, {"PubMed.DOI", "DOI"}}),
        Removed = Table.RemoveColumns(
          Ren,
          {
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
          MissingField.Ignore
        ),
        Ren1 = Table.RenameColumns(Removed, {{"crossref.Type", "publication_type"}, {"crossref.Title", "title"}, {"crossref.Error", "Error"}}),
        Lower = Table.TransformColumns(Ren1, {{"title", Text.Lower}}),
        Removed2 = Table.RemoveColumns(Lower, {"DOI"}, MissingField.Ignore),
        Ren2 = Table.RenameColumns(Removed2, {{"crossref.DOI", "crossref.doi"}})
      in
        Ren2,
    _input = (SourseTable as table) =>
      let
        Source1 = SourseTable,
        SourcePubMed0 = _PubMed(Source1),
        SourcePubMed = Table.RemoveColumns(SourcePubMed0, {"ChEMBL.document_chembl_id"}, MissingField.Ignore),
        SourceScholar = _scholar(Source1),
        SourceChEMBL = _ChEMBL(Source1),
        SourceOpenAlex = _OpenAlex(Source1),
        SourceCrossRef =  Table.TransformColumnTypes(Table.DuplicateColumn(_crossref(Source1), "PMID", "crossref.PMID"),{{"crossref.PMID", type text}}),
        J1 = Table.NestedJoin(SourcePubMed, {"PMID"}, SourceChEMBL, {"PMID"}, "ChEMBL"),
        E1 = Table.ExpandTableColumn(
          J1,
          "ChEMBL",
          {"title", "abstract", "ChEMBL.doi", "volume", "issue", "page", "authors", "document_chembl_id"},
          {
            "ChEMBL.title",
            "ChEMBL.abstract",
            "ChEMBL.doi",
            "ChEMBL.volume",
            "ChEMBL.issue",
            "ChEMBL.page",
            "authors",
            "ChEMBL.document_chembl_id"
          }
        ),
        J2 = Table.NestedJoin(E1, {"PMID"}, SourceScholar, {"scholar.PMID"}, "Scholar"),
        E2 = Table.ExpandTableColumn(
          J2,
          "Scholar",
          {"scholar.doi", "scholar.PublicationTypes", "scholar.Venue", "scholar.Error"},
          {"scholar.doi", "scholar.PublicationTypes", "scholar.Venue", "scholar.Error"}
        ),
        J3 = Table.NestedJoin(E2, {"PMID"}, SourceOpenAlex, {"OpenAlex.PMID"}, "OpenAlex"),
        E3 = Table.ExpandTableColumn(
          J3,
          "OpenAlex",
          {"OpenAlex.doi", "publication_type", "crossref_type", "OpenAlex.Genre", "OpenAlex.Venue", "MeSH.descriptors", "Error"},
          {"OpenAlex.doi", "OpenAlex.publication_type", "OpenAlex.crossref_type", "OpenAlex.Genre", "OpenAlex.Venue", "OpenAlex.MeSH.descriptors", "OpenAlex.Error"}
        ),
        J4 = Table.NestedJoin(E3, {"PMID"}, SourceCrossRef, {"crossref.PMID"}, "CrossRef"),
        E4 = Table.ExpandTableColumn(
          J4,
          "CrossRef",
          {"crossref.doi", "publication_type", "crossref.Subtype", "title", "crossref.Subtitle", "crossref.Subject", "Error"},
          {"crossref.doi", "crossref.publication_type", "crossref.crossref.Subtype", "crossref.title", "crossref.crossref.Subtitle", "crossref.crossref.Subject", "crossref.Error"}
        )
      in
        E4
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
          typed = Table.TransformColumnTypes(
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
// ===================== document_validation =====================
  data_validation_base = [
    // ===== get_validation =====
    // Назначение: валидация документарных записей и подготовка нормализованных атрибутов.
    get_validation = () as table =>
      let
        Source = Data[Document_out],
        Joined = document_input[_input](Source),
        Validated = Validation[ValidateAll](Joined),
        AddInvalid = Table.AddColumn(
          Validated,
          "invalid_record",
          each ([consensus_support] <= 2) or ([invalid_issue] = true) or ([invalid_volume] = true),
          type logical
        ),
        DropOriginals = Table.RemoveColumns(AddInvalid, {"title", "abstract", "volume", "issue", "page"}, MissingField.Ignore),
        DropNoise = Table.RemoveColumns(DropOriginals, {"ChEMBL.doi", "scholar.doi", "OpenAlex.doi", "crossref.doi"}, MissingField.Ignore),
        RenameNew = Table.RenameColumns(
          DropNoise,
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
        AddCompleted = Table.AddColumn(
          RenameNew,
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
        AddSort = Table.AddColumn(
          AddCompleted,
          "sort_order",
          each [ISSN] & ":" & [completed] & ":" & Text.PadStart(Text.From([PMID]), 8, "0"),
          type text
        ),
        DropVerbose = Table.RemoveColumns(
          AddSort,
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
        Ordered = Table.ReorderColumns(
          DropVerbose,
          List.Sort(Table.ColumnNames(DropVerbose))
        )
      in
        Ordered,

    // ===== get_document =====
    // Назначение: возврат очищенной таблицы документов для downstream-потребителей.
    get_document = () =>
      let
        Source = get_validation(),
        #"Changed Type" = Table.TransformColumnTypes(Source, {{"volume", type text}, {"issue", type text}}),
        #"Removed Columns" = Table.RemoveColumns(
          #"Changed Type",
          {
            "PubMed.doi",
            "error",
            "ChEMBL.title",
            "ChEMBL.abstract",
            "ChEMBL.volume",
            "ChEMBL.issue",
            "ChEMBL.page",
            "scholar.Venue",
            "scholar.Error",
            "OpenAlex.Error",
            "crossref.title",
            "crossref.Error",
            "selected_source",
            "invalid_volume",
            "invalid_issue",
            "PMID_for_validation"
          },
          MissingField.Ignore
        ),
        #"Renamed Columns" = Table.RenameColumns(#"Removed Columns", {{"abstract_", "abstract"}, {"_title", "title"}}),
        #"Reordered Columns1" = Table.ReorderColumns(
          #"Renamed Columns",
          {
            "PMID",
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
            "authors",
            "scholar.PublicationTypes",
            "OpenAlex.publication_type",
            "OpenAlex.crossref_type",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeSH.descriptors",
            "crossref.publication_type",
            "crossref.crossref.Subtype",
            "crossref.crossref.Subtitle",
            "crossref.crossref.Subject",
            "doi",
            "title",
            "abstract",
            "page",
            "volume",
            "issue",
            "invalid_record",
            "completed",
            "sort_order",
            "ISSN",
            ".journal"
          },
          MissingField.Ignore
        ),
        #"Reordered Columns" = Table.ReorderColumns(
          #"Reordered Columns1",
          {
            "PMID",
            "ISSN",
            ".journal",
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
            "authors",
            "scholar.PublicationTypes",
            "OpenAlex.publication_type",
            "OpenAlex.crossref_type",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeSH.descriptors",
            "crossref.publication_type",
            "crossref.crossref.Subtype",
            "crossref.crossref.Subtitle",
            "crossref.crossref.Subject",
            "doi",
            "title",
            "abstract",
            "volume",
            "issue",
            "page",
            "invalid_record",
            "completed",
            "sort_order"
          },
          MissingField.Ignore
        ),
        #"Added Custom" = Table.AddColumn(
          #"Reordered Columns",
          "reference",
          each [#".journal"] & " (ISSN=" & [ISSN] & "), " & [completed.year] & ", " & [volume] & " (" & [issue] & "), p:" & [page]
        ),
        #"Removed Columns2" = Table.RemoveColumns(
          #"Added Custom",
          {
            "ISSN",
            ".journal",
            "completed.year",
            "completed.month",
            "completed.day",
            "revised.year",
            "revised.month",
            "revised.day",
            "volume",
            "issue",
            "page"
          },
          MissingField.Ignore
        ),
        #"Reordered Columns2" = Table.ReorderColumns(
          #"Removed Columns2",
          {
            "invalid_record",
            "reference",
            "completed",
            "sort_order",
            "PMID",
            "publication_type",
            "MeSH.descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "authors",
            "scholar.PublicationTypes",
            "OpenAlex.publication_type",
            "OpenAlex.crossref_type",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeSH.descriptors",
            "crossref.publication_type",
            "crossref.crossref.Subtype",
            "crossref.crossref.Subtitle",
            "crossref.crossref.Subject",
            "doi",
            "title",
            "abstract"
          },
          MissingField.Ignore
        ),
        #"Removed Columns3" = Table.RemoveColumns(#"Reordered Columns2", {"crossref.crossref.Subtype", "crossref.crossref.Subtitle", "crossref.crossref.Subject"}, MissingField.Ignore),
        #"Reordered Columns4" = Table.ReorderColumns(
          #"Removed Columns3",
          {
            "doi",
            "invalid_record",
            "reference",
            "completed",
            "sort_order",
            "PMID",
            "publication_type",
            "MeSH.descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "authors",
            "scholar.PublicationTypes",
            "OpenAlex.publication_type",
            "OpenAlex.crossref_type",
            "OpenAlex.Genre",
            "OpenAlex.Venue",
            "OpenAlex.MeSH.descriptors",
            "crossref.publication_type",
            "title",
            "abstract"
          },
          MissingField.Ignore
        ),
        #"Reordered Columns3" = Table.ReorderColumns(
          #"Reordered Columns4",
          {
            "PMID",
            "reference",
            "doi",
            "sort_order",
            "completed",
            "invalid_record",
            "title",
            "abstract",
            "authors",
            "MeSH.descriptors",
            "OpenAlex.MeSH.descriptors",
            "PubMed.MeSH_Qualifiers",
            "PubMed.ChemicalList",
            "publication_type",
            "scholar.PublicationTypes",
            "OpenAlex.publication_type",
            "OpenAlex.crossref_type",
            "crossref.publication_type",
            "OpenAlex.Genre",
            "OpenAlex.Venue"
          },
          MissingField.Ignore
        ),
        #"Renamed Columns2" = Table.RenameColumns(#"Reordered Columns3", {{"PubMed.ChemicalList", "chemical_list"}}),
        #"Renamed Columns1" = Table.RenameColumns(
          #"Renamed Columns2",
          {
            {"PubMed.MeSH_Qualifiers", "MeSH.qualifiers"},
            {"MeSH.descriptors", "PubMed.MeSH"},
            {"OpenAlex.MeSH.descriptors", "OpenAlex.MeSH"},
            {"publication_type", "PubMed.publication_type"}
          }
        ),
        #"1Changed Type1" = Table.TransformColumnTypes(#"Renamed Columns1", {{"PMID", type text}}),
        a = citations[get_reference](),
        #"1Merged Queries" = Table.NestedJoin(#"1Changed Type1", {"PMID"}, a, {"pubmed_id"}, "NewColumn"),
        #"1Expanded NewColumn" = Table.ExpandTableColumn(
          #"1Merged Queries",
          "NewColumn",
          {"classification", "document_contains_external_links", "is_experimental_doc"},
          {"review", "document_contains_external_links", "is_experimental_doc"}
        ),
        #"1Replaced Value" = Table.ReplaceValue(#"1Expanded NewColumn", "", 0, Replacer.ReplaceValue, {"review"}),
        #"1Changed Type2" = Table.TransformColumnTypes(#"1Replaced Value", {{"review", Int64.Type}}),
        #"1Changed Type" = Table.TransformColumnTypes(#"1Changed Type2", {{"review", type logical}}),
        WithDocumentId =
          if List.Contains(Table.ColumnNames(#"1Changed Type"), "ChEMBL.document_chembl_id") then
            #"1Changed Type"
          else
            Table.AddColumn(#"1Changed Type", "ChEMBL.document_chembl_id", each null, type text),
        #"1Merged Queries1" = Table.NestedJoin(WithDocumentId, {"ChEMBL.document_chembl_id"}, citations[get_citations_fraction](), {"document_chembl_id"}, "NewColumn"),
        #"1Expanded NewColumn1" = Table.ExpandTableColumn(
          #"1Merged Queries1",
          "NewColumn",
          {"n_activity", "citations", "n_assay", "n_testitem", "significant_citations_fraction"},
          {"n_activity", "citations", "n_assay", "n_testitem", "significant_citations_fraction"}
        ),
        Keep = Table.SelectColumns(
          #"1Expanded NewColumn1",
          {
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
            "PubMed.publication_type",
            "ChEMBL.document_chembl_id",
            "scholar.PublicationTypes",
            "OpenAlex.publication_type",
            "OpenAlex.crossref_type",
            "OpenAlex.Genre",
            "crossref.publication_type",
            "review",
            "document_contains_external_links",
            "significant_citations_fraction",
            "is_experimental_doc",
            "n_activity",
            "citations",
            "n_assay",
            "n_testitem"
          },
          MissingField.Ignore
        ),
        Typed = Table.TransformColumnTypes(Keep, {{"PMID", Int64.Type}, {"review", type logical}, {"significant_citations_fraction", type logical}}),
        Drop_Journalish = {"journal-article", "journal article", "article", "journal"},
        Drop_Supportish =
          {
            "research support, u.s. gov't, p.h.s.",
            "research support, non-u.s. gov't",
            "research support, u.s. gov't, non-p.h.s.",
            "research support, n.i.h., extramural",
            "research support, n.i.h., intramural",
            "research support, american recovery and reinvestment act"
          },
        Alias_PublicationType =
          [
            #"clinical trial, phase i" = "clinical trial",
            #"clinical trial, phase ii" = "clinical trial",
            #"historical article" = "review",
            #"validation study" = "validation study",
            lecture = "review",
            address = "review",
            #"comparative study" = "comparative study"
          ],
        Alias_Scholar = [study = null, clinicaltrial = "clinicaltrial", review = "review", lettersandcomments = "lettersandcomments"],
        Alias_OpenAlexPubType = [paratext = "paratext", component = "paratext", article = null, journal = null, #"journal-article" = null, #"journal article" = null],
        Alias_OpenAlexXrefType = [#"journal-article" = null, article = null],
        Alias_OpenAlexGenre = [article = null, #"0" = null],
        Alias_CrossrefPubType = [#"journal-article" = null, article = null],
        ToTextAll = Table.TransformColumnTypes(
          Typed,
          {
            {"PubMed.publication_type", type text},
            {"ChEMBL.document_chembl_id", type text},
            {"scholar.PublicationTypes", type text},
            {"OpenAlex.publication_type", type text},
            {"OpenAlex.crossref_type", type text},
            {"OpenAlex.Genre", type text},
            {"crossref.publication_type", type text}
          },
          "en-US"
        ),
        Clean1 = Table.TransformColumns(
          ToTextAll,
          {
            {"scholar.PublicationTypes", each let t = CleanPipe(_, Alias_Scholar, {"journalarticle", "study", ""}, false) in t, type text}
          }
        ),
        Clean2 = Table.TransformColumns(
          Clean1,
          {
            {"OpenAlex.publication_type", each CleanPipe(_, Alias_OpenAlexPubType, {"0", "article", "journal", "journal-article", "journal article", ""}, false), type text}
          }
        ),
        Clean3 = Table.TransformColumns(
          Clean2,
          {
            {"crossref.publication_type", each CleanPipe(_, Alias_CrossrefPubType, {"journal-article", "article", ""}, false), type text}
          }
        ),
        Clean4 = Table.TransformColumns(
          Clean3,
          {
            {"OpenAlex.crossref_type", each CleanPipe(_, Alias_OpenAlexXrefType, {"journal-article", "article", ""}, false), type text}
          }
        ),
        Clean5 = Table.TransformColumns(
          Clean4,
          {
            {"OpenAlex.Genre", each CleanPipe(_, Alias_OpenAlexGenre, {"0", "article", ""}, false), type text}
          }
        ),
        Clean6 = Table.TransformColumns(
          Clean5,
          {
            {"PubMed.publication_type", each let drop = List.Union({Drop_Journalish, Drop_Supportish, {""}}) in CleanPipe(_, Alias_PublicationType, drop, false), type text}
          }
        ),
        Final = Table.ReorderColumns(
          Clean6,
          {
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
            "review",
            "significant_citations_fraction"
          },
          MissingField.Ignore
        ),
        #"Added Custom2" = Table.AddColumn(
          Final,
          "n_responces",
          each
            Number.From([PubMed.publication_type] <> "") +
            Number.From([scholar.PublicationTypes] <> "") +
            Number.From([OpenAlex.publication_type] <> "") +
            Number.From([OpenAlex.crossref_type] <> "") +
            2
        ),
        #"Added Custom1" = Table.AddColumn(
          #"Added Custom2",
          "updated_review",
          each
            [review] or
            (
              Number.From(Text.Contains([PubMed.publication_type], "review")) +
              Number.From([scholar.PublicationTypes] = "review") +
              Number.From([OpenAlex.publication_type] = "review") +
              Number.From([OpenAlex.crossref_type] = "review") +
              Number.From([review]) * 2
            ) / [n_responces] > 0.335
        ),
        #"Removed Columns1" = Table.RemoveColumns(#"Added Custom1", {"review"}, MissingField.Ignore),
        #"Renamed Columns3" = Table.RenameColumns(#"Removed Columns1", {{"updated_review", "review"}}),
        #"Added Custom3" = Table.AddColumn(#"Renamed Columns3", "is_experimental", each not [review])
      in
        #"Added Custom3",
    // ===== get_testitem =====
    // Назначение: объединение тестируемых сущностей с эталонным справочником и расчёт флагов качества.
    get_testitem = () =>
      let
        get_reference = () =>
          let
            Source = Data[Testitem_in],
            #"Changed Type" = Table.TransformColumnTypes(
              Source,
              {
                {"molecule_chembl_id", type text},
                {"all_names", type text},
                
                {"nstereo", Int64.Type}
                 
              }
            )
          in
            #"Changed Type",
        Source = Data[Testitem_out],

        #"Merged Queries" = Table.NestedJoin(Source, {"molecule_chembl_id"}, get_reference(), {"molecule_chembl_id"}, "NewColumn"),
        #"Expanded NewColumn" = Table.ExpandTableColumn(
          #"Merged Queries",
          "NewColumn",
          {"all_names", "nstereo"},
          {"all_names", "nstereo"}
        ),
        #"Lowercased Text" = Table.TransformColumns(#"Expanded NewColumn", {{"synonyms", Text.Lower}, {"all_names", Text.Lower}}),
        #"Replaced Value" = Table.ReplaceValue(#"Lowercased Text", """]", "", Replacer.ReplaceText, {"synonyms"}),
        #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value", "[""", "", Replacer.ReplaceText, {"synonyms"}),
        #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1", """", "|", Replacer.ReplaceText, {"synonyms"}),
        #"Lowercased Text1" = Table.TransformColumns(#"Replaced Value2", {{"pref_name", Text.Lower}}),
        #"Added to Column" = Table.TransformColumns(#"Lowercased Text1", {{"nstereo", each List.Sum({_, -1}), Int64.Type}}),
        #"Changed Type1" = Table.TransformColumnTypes(#"Added to Column", {{"nstereo", type logical}}),
        #"Renamed Columns" = Table.RenameColumns(#"Changed Type1", {{"nstereo", "unknown_chirality"}}),
        #"Added Custom" = Table.AddColumn(#"Renamed Columns", "invalid_record", each not ([molecule_type] = "Small molecule" and [structure_type] = "MOL" and [is_radical] = false and [standard_inchi_key] <> "")),
        Ordered = Table.ReorderColumns(
          #"Added Custom",
          List.Sort(Table.ColumnNames(#"Added Custom"))
        )
      in
        Ordered,
    // ===== get_assay =====
    // Назначение: типизация и очистка витрины биологических тестов.
    get_assay = () =>
      let
        Source = Data[Assay_out],
        #"Changed Type" = Table.TransformColumnTypes(
          Source,
          {
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
          }
        ),
        #"Removed Columns" = Table.RemoveColumns(#"Changed Type", {"assay_tax_id", "confidence_score", "confidence_description", "relationship_type", "relationship_description", "assay_strain"}, MissingField.Ignore),
        Ordered = Table.ReorderColumns(
          #"Removed Columns",
          List.Sort(Table.ColumnNames(#"Removed Columns"))
        )
      in
        Ordered
  ],

  IUPHAR_int = () =>
    let
      #"Removed Columns" = RemoveIfExists(
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
      #"Removed Columns1" = RemoveIfExists(
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
// Helpers (общие)
// =====================================================
  RemoveIfExists = (tbl as table, cols as list) as table =>
    let
      keep = List.Difference(Table.ColumnNames(tbl), cols)
    in
      Table.SelectColumns(tbl, keep, MissingField.Ignore),
  SplitPipesToList = (x as any) as list =>
    let
      txt = ToText(x),
      parts = List.Transform(Text.Split(txt, "|"), each Text.Trim(_)),
      out = List.Select(parts, each _ <> "")
    in
      out,
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
        withClass =
          Table.AddColumn(
            src,
            "cellularity",
            each
              let
                sk = try Record.Field(_, superkingdomColumn) otherwise null,
                ph = try Record.Field(_, phylumColumn) otherwise null,
                guess = ClassifyByLineage(sk, ph)
              in
                guess,
            type text
          )
      in
        withClass
  ],
// =====================================================
// PTM helpers bundle
// =====================================================
  PTM = [
    ParseAAList = (src as table, columnName as text, mapToken as function, optional keepPos as nullable logical) as table =>
      let
        X0 = RemoveIfExists(src, {}),
        col = columnName,
        L0 = X0{0}?,
        V0 = X0[col],
        T0 = Table.FromList(V0, Splitter.SplitByNothing(), {"raw"}),
        T1 = Table.AddIndexColumn(T0, "Index", 0, 1),
        T2 = Table.SelectRows(T1, each [raw] <> null and Text.Trim(ToText([raw])) <> ""),
        T3 = Table.TransformColumns(T2, {{"raw", each List.Select(SplitPipesToList(_), each _ <> ""), type list}}),
        T4 = Table.ExpandListColumn(T3, "raw"),
        T5 = Table.SplitColumn(T4, "raw", Splitter.SplitTextByEachDelimiter({"["}, null, false), {"token", "posRaw"}),
        T6 = Table.TransformColumns(T5, {{"token", each Text.Lower(Text.Trim(ToText(_))), type text}, {"posRaw", each Text.Trim(Text.Replace(ToText(_), "]", "")), type text}}),
        T7 = Table.TransformColumns(T6, {{"token", each mapToken(_), type text}}),
        T8 = if keepPos = null or keepPos = true then Table.AddColumn(T7, "AA", each [token] & [posRaw], type text) else Table.RenameColumns(T7, {{"token", "AA"}}),
        G = Table.Group(T8, {"Index"}, {{"Count", each Table.RowCount(_), Int64.Type}, {"aa_list", each Text.Combine(List.RemoveNulls([AA]), "|"), type text}})
      in
        G,
    MapPhospho = (s as text) as text => if s = "phosphoserine" then "pS" else if s = "phosphothreonine" then "pT" else if s = "phosphotyrosine" then "pY" else s,
    MapAA1 = (s as text) as text =>
      let
        repl =
          [
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
        key = Text.Lower(Text.Trim(s)),
        out = if Record.HasFields(repl, key) then Record.Field(repl, key) else key
      in
        out
  ],

// =====================================================
// target record (модуль функций)
// =====================================================
  target = [
    isoform = (Source as table) as table =>
      let
        T1 = SelectColumnsSafe(Source, {"isoform_synonyms", "isoform_names", "isoform_ids", "uniprot_id_primary", "target_chembl_id"}),
        T2 = Table.TransformColumns(T1, {{"isoform_synonyms", each Text.Lower(ToText(_)), type text}, {"isoform_names", each Text.Lower(ToText(_)), type text}}),
        T3 = Table.TransformColumns(T2, {{"isoform_synonyms", each SplitPipesToList(_), type list}, {"isoform_names", each SplitPipesToList(_), type list}, {"isoform_ids", each SplitPipesToList(_), type list}}),
        MakeTriples = (names as list, ids as list, syns as list) as list =>
          let
            n = List.Max({List.Count(names), List.Count(ids), List.Count(syns)}),
            idx = if n = 0 then {} else {0 .. n - 1},
            recs = List.Transform(idx, (i) => [name = if i < List.Count(names) then names{i} else null, id = if i < List.Count(ids) then ids{i} else null, synonym = if i < List.Count(syns) then syns{i} else null])
          in
            recs,
        WithTriples = Table.AddColumn(T3, "triples", each MakeTriples([isoform_names], [isoform_ids], [isoform_synonyms]), type list),
        Rows = Table.ExpandListColumn(WithTriples, "triples"),
        Expanded = Table.ExpandRecordColumn(Rows, "triples", {"name", "id", "synonym"}),
        SynExpand = (s as any) as list =>
          let
            t = Text.Trim(Text.Lower(ToText(s))),
            v = List.Distinct(List.Select({t, Text.Replace(t, "pde", ""), Text.Replace(t, "pld", "")}, each _ <> ""))
          in
            v,
        WithTokens = Table.AddColumn(
          Expanded,
          "tokens",
          each
            let
              parts = List.Transform(Text.Split(ToText([synonym]), ":"), each Text.Trim(_)),
              nonEmpty = List.Select(parts, each _ <> ""),
              variants = List.Distinct(List.Combine(List.Transform(nonEmpty, each SynExpand(_))))
            in
              variants,
          type list
        ),
        Names = SelectColumnsSafe(Expanded, {"id", "uniprot_id_primary", "target_chembl_id", "name"}),
        NamesClean = Table.SelectRows(Table.TransformColumns(Names, {{"name", each Text.Trim(ToText(_)), type text}}), each [name] <> "" and [name] <> "n/a" and [name] <> "none"),
        Syns = SelectColumnsSafe(WithTokens, {"id", "uniprot_id_primary", "target_chembl_id", "tokens"}),
        SynsExploded = Table.ExpandListColumn(Syns, "tokens"),
        SynsClean = Table.RenameColumns(Table.SelectRows(Table.TransformColumns(SynsExploded, {{"tokens", each Text.Trim(ToText(_)), type text}}), each [tokens] <> "" and [tokens] <> "n/a" and [tokens] <> "none"), {{"tokens", "name"}}),
        Combined = Table.Combine({NamesClean, SynsClean}),
        Dedup = Table.Distinct(Combined, {"id", "name", "target_chembl_id", "uniprot_id_primary"}),
        Sorted = Table.Sort(Dedup, {{"uniprot_id_primary", Order.Ascending}, {"id", Order.Ascending}}),
        Out = Table.Distinct(Sorted, {"id", "name"})
      in
        Out,
    crossref = (Source as table) as table =>
      let
        drop =
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
        Out = RemoveIfExists(Source, drop)
      in
        Out,
    activity = (Source as table) as table =>
      let
        drop =
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
            "secondaryAccessions",
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
        X = RemoveIfExists(Source, drop),
        Out = Table.SelectRows(X, each ToText([reaction_ec_numbers]) <> "")
      in
        Out,
    metadata = (Source as table) as table =>
      let
        drop1 =
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
        R0 = RemoveIfExists(Source, drop1),
        keepOrder =
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
        R1 = Table.SelectColumns(R0, List.Intersect({Table.ColumnNames(R0), keepOrder}), MissingField.Ignore),
        drop2 =
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
          },
        Out = RemoveIfExists(R1, drop2)
      in
        Out,
    organism = (Source as table) as table =>
      let
        base = SelectColumnsSafe(Source, {"target_chembl_id", "uniprot_id_primary", "organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "reaction_ec_numbers"}),
        lower = Table.TransformColumns(base, {{"lineage_superkingdom", Text.Lower, type text}, {"lineage_phylum", Text.Lower, type text}, {"lineage_class", Text.Lower, type text}}),
        withCell = Cellularity_[AddCellularitySmart](lower, "taxon_id", "lineage_superkingdom", "lineage_class"),
        ecMainList = Table.AddColumn(withCell, "ec_major_list", each let items = SplitPipesToList([reaction_ec_numbers]) in List.Distinct(List.Transform(items, each if Text.Contains(_, ".") then Text.Split(_, "."){0} else _)), type list),
        multifn = Table.AddColumn(ecMainList, "multifunctional_enzyme", each List.Count([ec_major_list]) > 1, type logical),
        out = Table.RemoveColumns(multifn, {"ec_major_list"}, MissingField.Ignore)
      in
        out,

    name = (Source as table) as table =>
      let
        drop =
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
            "InterPro",
            "Pfam",
            "PRINTS",
            "TCDB",
            "target_type",
            "protein_classifications",
            "sequence_length",
            "features_transmembrane",
            "features_topology",
            "ptm_glycosylation",
            "ptm_lipidation",
            "ptm_disulfide_bond",
            "ptm_modified_residue"
          },
        R0 = RemoveIfExists(Source, drop),
        keep =
          {
            "target_chembl_id",
            "recommendedName",
            "geneName",
            "pref_name",
            "uniprot_id_primary",
            "gene_symbol",
            "protein_name_canonical",
            "protein_name_alt",
            "xref_iuphar",
            "gtop_target_id",
            "uniprot_last_update",
            "uniprot_version",
            "pipeline_version",
            "timestamp_utc",
            "secondaryAccessions",
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
            "GuidetoPHARMACOLOGY",
            "target_components",
            "cross_references",
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
            "iuphar_full_name_path"
          },
        R1 = Table.SelectColumns(R0, List.Intersect({Table.ColumnNames(R0), keep}), MissingField.Ignore),
        R2 = Table.TransformColumns(R1, {{"gene_symbol_list", each Text.Lower(Text.Replace(Text.Replace(ToText(_), "[""", ""), """]", "")), type text}, {"protein_name_alt", each let s = Text.Replace(Text.Replace(ToText(_), "[""", ""), """]", "") in if s = "[]" or s = "" then null else s, type nullable text}}),
        TComp = try Table.SplitColumn(R2, "target_components", Splitter.SplitTextByEachDelimiter({"""component_description"": """}, QuoteStyle.None, false), {"tc1", "tc2"}) otherwise R2,
        TComp2 = try Table.SplitColumn(TComp, "tc2", Splitter.SplitTextByEachDelimiter({""", """}, QuoteStyle.None, false), {"tc2a", "tc2b"}) otherwise TComp,
        TComp3 = RemoveIfExists(TComp2, {"tc1", "tc2b"}),
        R3 = Table.ReorderColumns(
          TComp3,
          List.Intersect({{"uniprot_id_primary", "protein_name_alt", "tc2a", "recommendedName", "pref_name", "protein_name_canonical", "geneName", "gene_symbol_list"}, Table.ColumnNames(TComp3)}),
          MissingField.Ignore
        ),
        R4 = Table.TransformColumns(R3, {{"geneName", Text.Lower, type text}, {"gene_symbol_list", Text.Lower, type text}, {"protein_name_canonical", ToText, type text}, {"pref_name", ToText, type text}}),
        Syn = Table.AddColumn(R4, "synonyms", each let parts = List.RemoveNulls({[protein_name_canonical], [pref_name], [tc2a], [protein_name_alt], [gene_symbol_list]}) in Text.Combine(List.Distinct(List.Combine(List.Transform(parts, SplitPipesToList))), "|"), type text),
        Out0 = Table.RenameColumns(RemoveIfExists(Syn, {"protein_class_pred_L1", "protein_class_pred_L2", "protein_class_pred_L3", "protein_class_pred_rule_id", "protein_class_pred_evidence", "protein_class_pred_confidence", "iuphar_target_id", "iuphar_family_id", "iuphar_type", "iuphar_class", "iuphar_subclass", "iuphar_chain", "iuphar_name", "iuphar_full_id_path", "iuphar_full_name_path", "cross_references", "reactions", "reaction_ec_numbers"}), {{"recommendedName", "recommended_name"}, {"geneName", "gene_name"}})
      in
        Out0,
    main = (Source as table) as table =>
      let
        drop =
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
        R0 = RemoveIfExists(Source, drop),
        keepOrder =
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
        R1 = Table.SelectColumns(R0, List.Intersect({Table.ColumnNames(R0), keepOrder}), MissingField.Ignore),
        R2 = RemoveIfExists(R1, {"uniprot_last_update", "uniprot_version", "pipeline_version", "timestamp_utc", "geneName", "xref_iuphar", "gtop_target_id", "GuidetoPHARMACOLOGY", "sequence_length", "features_transmembrane", "features_topology", "molecular_function", "cellular_component", "subcellular_location", "topology"}),
        Jn = Table.NestedJoin(R2, {"target_chembl_id", "uniprot_id_primary"}, name(Source), {"target_chembl_id", "uniprot_id_primary"}, "nm", JoinKind.LeftOuter),
        Ex = Table.ExpandTableColumn(Jn, "nm", {"recommended_name", "gene_name", "synonyms"}, {"recommended_name", "gene_name", "synonyms"}),
        Out = RemoveIfExists(Table.TransformColumns(Ex, {{"gene_name", Text.Lower, type text}}), {"protein_name_canonical", "gene_symbol"})
      in
        Out,
    PTM = [
      phosphorylation = (Source as table) as table => PTM[ParseAAList](RemoveIfExists(Source, {}), "phosphorylation", PTM[MapPhospho], true),
      acetylation = (Source as table) as table => PTM[ParseAAList](RemoveIfExists(Source, {}), "acetylation", PTM[MapAA1], true),
      glycosylation = (Source as table) as table => PTM[ParseAAList](RemoveIfExists(Source, {}), "glycosylation", (s) => if s = "asparagine" then "N" else if s = "serine" then "S" else if s = "threonine" then "T" else PTM[MapAA1](s), true),
      modified_residue = (Source as table) as table => PTM[ParseAAList](RemoveIfExists(Source, {}), "modified_residue", PTM[MapAA1], true),
      disulfide_bond = (Source as table) as table => let T = PTM[ParseAAList](RemoveIfExists(Source, {}), "disulfide_bond", (s) => s, true) in Table.RenameColumns(T, {{"Count", "n_disulfide_bond"}, {"aa_list", "disulfide_bond_list"}}),
      propeptide = (Source as table) as table =>
        let
          MapProp = (s as text) as text =>
            let
              t =
                Text.Replace(
                  Text.Replace(
                    Text.Replace(
                      Text.Replace(
                        Text.Replace(
                          Text.Replace(
                            Text.Replace(
                              Text.Replace(
                                Text.Replace(
                                  Text.Replace(Text.Lower(s), "removed in mature form", "del"),
                                  "n-terminally processed",
                                  "del"
                                ),
                                "removed by plasmin",
                                "del"
                              ),
                              "removed by furin",
                              "del"
                            ),
                            "removed for receptor activation",
                            "del"
                          ),
                          "removed by bmp1",
                          "del"
                        ),
                        "activation peptide (connecting region)",
                        "act"
                      ),
                      "activation peptide",
                      "act"
                    ),
                    "inhibition peptide",
                    "act"
                  ),
                  "linker peptide",
                  "lnk"
                )
            in
              Text.Replace(t, "interdomain linker", "lnk"),
          T = PTM[ParseAAList](RemoveIfExists(Source, {}), "propeptide", MapProp, false)
        in
          Table.RenameColumns(T, {{"Count", "n_propeptide"}, {"aa_list", "propeptide_list"}}),
      lipidation = (Source as table) as table =>
        let
          map = (s as text) as text => PTM[MapAA1](s),
          T = PTM[ParseAAList](RemoveIfExists(Source, {}), "lipidation", map, true)
        in
          Table.RenameColumns(T, {{"Count", "n_lipidation"}, {"aa_list", "lipidation_list"}})
    ],

    IUPHAR = () as table =>
      let
        NormalizeTable19 = () as table =>
          let
            T0 = table19(),
            T1 = Table.TransformColumnTypes(
              T0,
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
            C1 = Table.ReplaceValue(T1, "Enzyme: Protease", "Enzyme", Replacer.ReplaceText, {"Column1"}),
            C2 = Table.ReplaceValue(C1, "Enzyme: Kinase", "Enzyme", Replacer.ReplaceText, {"Column1"}),
            C3 = Table.ReplaceValue(C2, "Ion Channel: Voltage-gated", "Voltage-gated", Replacer.ReplaceText, {"Column2"}),
            C4 = Table.ReplaceValue(C3, "Ion Channel: Ligand-gated", "Ligand-gated", Replacer.ReplaceText, {"Column2"}),
            Out = Table.SelectRows(C4, each [Column1] <> null)
          in
            Out,
        T19 = Table.Buffer(NormalizeTable19()),
        IupharIdCols = {"iuphar_target_id", "iuphar_family_id", "iuphar_type", "iuphar_class", "iuphar_subclass", "iuphar_chain", "iuphar_full_id_path", "iuphar_full_name_path"},
        PredCols = {"protein_class_pred_L1", "protein_class_pred_L2", "protein_class_pred_L3", "protein_class_pred_rule_id", "protein_class_pred_evidence", "protein_class_pred_confidence"},
        IUPHAR_All = List.Combine({IupharIdCols, PredCols}),
        A0 = internal_target[main](Data[Target_out]),
        JOrg = Table.NestedJoin(A0, {"target_chembl_id"}, internal_target[organism](Data[Target_out]), {"target_chembl_id"}, "org", JoinKind.LeftOuter),
        EOrg = Table.ExpandTableColumn(JOrg, "org", {"organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme"}, {"organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme"}),
        JAct = Table.NestedJoin(EOrg, {"target_chembl_id", "uniprot_id_primary"}, internal_target[activity](Data[Target_out]), {"target_chembl_id", "uniprot_id_primary"}, "act", JoinKind.LeftOuter),
        EAct = Table.ExpandTableColumn(JAct, "act", {"reaction_ec_numbers"}, {"ec_number"}),
        MergeIUPHAR = (src as table) as table =>
          let
            src0 = RemoveIfExists(src, IUPHAR_All),
            j = Table.NestedJoin(src0, {"target_chembl_id"}, IUPHAR_int(), {"target_chembl_id"}, "IUPHAR", JoinKind.LeftOuter),
            e = Table.ExpandTableColumn(j, "IUPHAR", IUPHAR_All, IUPHAR_All)
          in
            e,
        MDir = MergeIUPHAR(EAct),
        Base = Table.SelectRows(MDir, each ToText([iuphar_family_id]) <> "N/A" or ToText([cellularity]) = "unicellular"),
        BaseDrop = RemoveIfExists(Base, {"gene_name", "synonyms", "organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme", "ec_number"}),
        ran5 = (S as table) as table =>
          let
            M1 = MergeIUPHAR(S),
            F0 = Table.SelectRows(M1, each ToText([iuphar_full_id_path]) = ""),
            F = RemoveIfExists(F0, IupharIdCols),
            J = Table.NestedJoin(F, {"protein_class_pred_L1"}, T19, {"iuphar_class"}, "T19", JoinKind.LeftOuter),
            E = Table.ExpandTableColumn(J, "T19", IupharIdCols & {"Column1", "Column2"}, IupharIdCols & {"Column1", "Column2"}),
            F2 = Table.SelectRows(E, each [Column2] <> null),
            Out = RemoveIfExists(F2, {"Column1", "Column2"})
          in
            Out,
        ran6 = (S as table) as table =>
          let
            M1 = MergeIUPHAR(S),
            F0 = Table.SelectRows(M1, each ToText([iuphar_full_id_path]) = ""),
            F = RemoveIfExists(F0, IupharIdCols),
            J = Table.NestedJoin(F, {"protein_class_pred_L2"}, T19, {"Column2"}, "T19", JoinKind.LeftOuter),
            E = Table.ExpandTableColumn(J, "T19", IupharIdCols & {"Column1", "Column2"}, IupharIdCols & {"Column1", "Column2"}),
            F2 = Table.SelectRows(E, each [iuphar_chain] <> null),
            D1 = Table.Distinct(F2, {"target_chembl_id", "uniprot_id_primary"}),
            Out = Table.Distinct(D1, {"target_chembl_id"})
          in
            Out,
        ran4 = (S as table) as table =>
          let
            M1 = MergeIUPHAR(S),
            F0 = Table.SelectRows(M1, each ToText([iuphar_family_id]) = "N/A" and ToText([cellularity]) = "multicellular" and [ec_number] <> null),
            S2 = Table.SplitColumn(F0, "ec_number", Splitter.SplitTextByEachDelimiter({"."}, null, false), {"ec_major", "ec_minor"}),
            F1 = Table.SelectRows(S2, each [ec_major] <> "3"),
            T = Table.TransformColumnTypes(F1, {{"ec_major", Int64.Type}, {"ec_minor", type text}}),
            J = Table.NestedJoin(T, {"ec_major"}, T19, {"ec"}, "T19", JoinKind.LeftOuter),
            Jnd = RemoveIfExists(J, IupharIdCols),
            E = Table.ExpandTableColumn(Jnd, "T19", IupharIdCols, IupharIdCols),
            App = Table.Combine({E, ran5(S), ran6(S)}),
            D = Table.Distinct(App, {"target_chembl_id"}),
            Drop = RemoveIfExists(D, {"secondaryAccessions", "gene_name", "synonyms", "organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme", "ec_major", "ec_minor", "Column1", "Column2"})
          in
            Drop,
        Known1 = Table.Combine({BaseDrop, ran4(EAct)}),
        Known_table = Table.Distinct(Known1, {"target_chembl_id"}),
        J0 = Table.NestedJoin(EAct, {"target_chembl_id"}, Known_table, {"target_chembl_id"}, "K", JoinKind.LeftOuter),
        E0 = Table.ExpandTableColumn(J0, "K", {"target_chembl_id"}, {"known_id"}),
        F0 = Table.SelectRows(E0, each [known_id] = null),
        D0 = Table.Distinct(F0, {"target_chembl_id"}),
        F1 = D0,
        F2 = Table.AddColumn(F1, "ec_key", each 100, Int64.Type),
        J1 = Table.NestedJoin(F2, {"ec_key"}, T19, {"ec"}, "T19", JoinKind.LeftOuter),
        #"Removed Columns" = RemoveIfExists(J1, {"iuphar_target_id", "iuphar_family_id", "iuphar_type", "iuphar_class", "iuphar_subclass", "iuphar_chain", "iuphar_name", "iuphar_full_id_path", "iuphar_full_name_path"}),
        E1 = Table.ExpandTableColumn(#"Removed Columns", "T19", IupharIdCols, IupharIdCols),
        Out1 = RemoveIfExists(E1, {"ec_number", "ec_key"}),
        All = Table.Combine({Known_table, Out1}),
        Final = Table.Distinct(All, {"target_chembl_id"}),
        Out = RemoveIfExists(Final, PredCols & {"ec_number", "gene_name", "synonyms", "organism", "taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "cellularity", "multifunctional_enzyme", "known_id"})
      in
        Out
  ],
  // ===== get_target =====
  // Назначение: объединение таргетных атрибутов и справочников IUPHAR.
  get_target = () =>
    let
      main1 = target[main](Data[Target_out]),
      #"Removed Columns" = RemoveIfExists(main1, {"protein_class_pred_L1", "protein_class_pred_L2", "protein_class_pred_L3", "protein_class_pred_rule_id", "protein_class_pred_evidence", "protein_class_pred_confidence", "iuphar_target_id", "iuphar_family_id", "iuphar_type", "iuphar_class", "iuphar_subclass", "iuphar_chain", "iuphar_name", "iuphar_full_id_path", "iuphar_full_name_path"}),
      #"Merged Queries1" = Table.NestedJoin(#"Removed Columns", {"target_chembl_id"}, target[organism](Data[Target_out]), {"target_chembl_id"}, "NewColumn.1"),
      aa = Table.ExpandTableColumn(#"Merged Queries1", "NewColumn.1", {"taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "reaction_ec_numbers", "cellularity", "multifunctional_enzyme"}, {"1.taxon_id", "lineage_superkingdom", "lineage_phylum", "lineage_class", "reaction_ec_numbers", "cellularity", "multifunctional_enzyme"}),
      #"2Merged Queries1" = Table.NestedJoin(aa, {"target_chembl_id"}, target[IUPHAR](), {"target_chembl_id"}, "NewColumn.1"),
      #"Expanded NewColumn.1" = Table.ExpandTableColumn(#"2Merged Queries1", "NewColumn.1", {"iuphar_name", "iuphar_target_id", "iuphar_family_id", "iuphar_type", "iuphar_class", "iuphar_subclass", "iuphar_chain", "iuphar_full_id_path", "iuphar_full_name_path"}, {"iuphar_name", "iuphar_target_id", "iuphar_family_id", "iuphar_type", "iuphar_class", "iuphar_subclass", "iuphar_chain", "iuphar_full_id_path", "iuphar_full_name_path"}),
      Ordered = Table.ReorderColumns(
        #"Expanded NewColumn.1",
        List.Sort(Table.ColumnNames(#"Expanded NewColumn.1"))
      )
    in
      Ordered,
  data_validation = Record.AddField(data_validation_base, "get_target", get_target),
  // ===== Exports =====
  // Назначение: единая точка доступа к публичным функциям модуля.
  Exports =
    [
      get_validation = data_validation[get_validation],
      get_document = data_validation[get_document],
      get_testitem = data_validation[get_testitem],
      get_assay = data_validation[get_assay],
      get_target = data_validation[get_target]
    ]
in
  [
    Exports = Exports,
    _document = Exports[get_document],
    _testitem = Exports[get_testitem],
    _assay = Exports[get_assay],
    _target = Exports[get_target],
    _validation = Exports[get_validation],
    ParamsSummary = ParamsSummary
  ]
