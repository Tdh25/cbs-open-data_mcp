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
1. `query_datasets(filter="contains(Title,'keyword')")` → find relevant table IDs.
2. `get_dimensions(dataset="IDHERE")` → list dimensions with human-readable labels.
3. `get_measure_codes(dataset="IDHERE")` → list available measures (value columns).
4. `get_observations(dataset="IDHERE")` → fetch all observations (auto-paginates).

### Parameter Naming
- The dataset parameter is always called `dataset` (not `id`).
- The catalog parameter defaults to `"CBS"` — omit unless querying a different catalog.

### Observations
- `get_observations()` auto-paginates and resolves labels by default.
- `resolve_labels=True` (default): dimension codes and measure codes are replaced by human-readable labels. Use this when presenting data to users or building visualizations.
- `resolve_labels=False`: raw codes only — faster, use when exploring structure, checking row counts, or when labels are not needed yet.
- `get_observations()` auto-paginates by default (follows `@odata.nextLink`).
- **Period codes (raw)**: annual = `2021JJ00`; quarterly = `2021KW01`–`2021KW04`.

### Presenting Results
- When presenting a dataset overview to the user, summarize results in a Markdown table with columns (e.g: `#`, `ID`, `Title`, `Description`, `Dimensions`, `Periods`).
- For visualization requests, always answer directly in chat using a Mermaid `xychart-beta` diagram — **avoid creating a notebook** when possible. 
- Always use the `renderMermaidDiagram` tool for rendering; writing a raw mermaid code block in chat text does not render as a diagram.
- `xychart-beta` supports multiple `line`/`bar` series but has **no built-in legend** and **no color customization**. Use colored emoji circles in text above or below the chart as an informal legend, matching Mermaid's actual default color order: 🔵🟡🟢🔴🩶 (positions 1–5). Colors 6+ are near-white/invisible — **never use more than 5 series**. Aggregate excess series into an "Overig" category if needed.
- Create a Jupyter notebook only when explicitly requested, or when the chart is stacked, needs interactivity, or aggregating series would distort the data beyond usefulness.

### Known Limitations
- CBS API is slow — timeout is 60s with 3 retries.
- SSL verification is disabled due to CBS certificate issues.
- `$search` on the Datasets endpoint is unreliable.
- OData `$filter` on `Perioden` is often ignored server-side — fetch all data and filter in Python instead.
