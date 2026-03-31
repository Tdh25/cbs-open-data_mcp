---
applyTo: "**/cbs_open_data*.py,**/*.ipynb"
---

## CBS Open Data MCP – Usage Guide

### Dataset Discovery
- **Always** use `filter` with `contains(Title,'...')` or `contains(Description,'...')` — never use `$search` (returns popularity-ranked, irrelevant results).
- Use Title search when the topic keyword likely appears in table titles; use Description search when the keyword is domain-specific and may only appear in the table description.
- Use **singular** search terms — CBS descriptions contain both singular and plural forms, so searching both is redundant.
- Use specific terms; generic terms like `'energie'` or `'verbruik'` return large, unfocused result sets.
- Combine filters for precision: `contains(Title,'energie') and (Status ne 'Gediscontinueerd')`.
- Discontinued datasets are excluded by default.

### Recommended Workflow
1. `query_datasets(filter="contains(Title,'keyword')")` → find relevant table IDs. Check `ObservationCount` in results.
2. *(Optional)* Explore structure when dimension names or measure units are unknown:
   - `get_dimensions(dataset="IDHERE")` → dimension names and available codes.
   - `get_measure_codes(dataset="IDHERE")` → measure identifiers and units.
3. Choose fetching strategy based on `ObservationCount`:
   - **< 10.000 obs**: `get_observations()` — auto-paginates, fetches everything.
   - **≥ 10.000 obs**: `query_observations(filter="...")` — applies server-side filtering. Always prefer this for large datasets.
4. If multiple candidate datasets exist (e.g. actueel vs. prognose), prefer the smallest one that still answers the question — server-side filtering on large CBS datasets is unreliable.

### Parameter Naming
- The dataset parameter is always called `dataset` (not `id`).
- The catalog parameter defaults to `"CBS"` — omit unless querying a different catalog.
- Omit `select` unless you know the exact field names; unknown field names in `select` cause a 400 error.

### Observations
- **Use `query_observations(filter="...")` when you need server-side filtering** — `get_observations(odata_filter=...)` does NOT reliably apply filters on large datasets.
- `get_observations()` auto-paginates and resolves labels by default; use only when fetching the entire (small) dataset.
- `resolve_labels=True` (default): replaces codes with human-readable labels. May silently fall back to raw codes on large datasets — verify the output.
- `resolve_labels=False`: raw codes only — faster, use when exploring structure or when labels are not needed yet.
- **Period codes (raw)**: annual = `2021JJ00`; quarterly = `2021KW01`–`2021KW04`.
- **Regio codes**: `NL01` = Nederland, `PV{nn}` = provincie, `CR{nn}` = COROP-gebied, `GM{nnnn}` = gemeente. Use these in `query_observations` filters (e.g. `RegioS eq 'PV20'`).
- **Units**: always check measure units via `get_measure_codes()` or dataset description before interpreting values. 

### Presenting Results
- When presenting a dataset overview to the user, summarize results in a Markdown table with columns (e.g: `#`, `ID`, `Title`, `Description`, `Dimensions`, `Periods`).
- For visualization requests, always answer directly in chat using a Mermaid `xychart-beta` diagram — **avoid creating notebooks or python files** when possible. 
- Always use the `renderMermaidDiagram` tool for rendering; writing a raw mermaid code block in chat text does not render as a diagram.
- `xychart-beta` supports multiple `line`/`bar` series but has **no built-in legend** and **no color customization**. Use colored emoji circles in text above or below the chart as an informal legend, matching Mermaid's default color order: 🔵🟢🔴🟡🩶 (positions 1–5). Colors 6+ are near-white/invisible — **never use more than 5 series**. Aggregate excess series into an "Overig" category if needed.
- Create a Jupyter notebook only when explicitly requested, or when the chart is stacked, needs interactivity, or aggregating series would distort the data beyond usefulness.

### Data Processing
- After fetching observations, process data in Python — not PowerShell. Python handles large JSON reliably, especially on paths with spaces.
- Filter annual periods (`2021JJ00` / label `"2021"`) in Python after fetching; OData `$filter` on `Perioden` is ignored server-side.

### Citing CBS Tables
- Always cite the source dataset as a hyperlink: `[Title (ID)](https://dataportal.cbs.nl/detail/CBS/{ID})`. Example: [Raming CO₂-emissie (84057NED)](https://dataportal.cbs.nl/detail/CBS/84057NED)
- Include the citation below charts or tables in the response.

### Known Limitations
- `get_dimension_values` often returns 404 — **do not use it**. Use `get_dimensions` instead; it already resolves codes to labels via `{dim}Codes` endpoints.
- For datasets with >50.000 observations, direct API filtering is infeasible. Find a smaller equivalent dataset or pre-aggregated variant.
- `get_observations(odata_filter=...)` ignores filters server-side on large datasets — use `query_observations(filter=...)` instead.
- CBS API is slow — timeout is 60s with 3 retries.
- SSL verification is disabled due to CBS certificate issues.
- `$search` on the Datasets endpoint is unreliable.
- OData `$filter` on `Perioden` is often ignored server-side — fetch all data and filter in Python instead.
