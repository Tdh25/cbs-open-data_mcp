"""MCP server for the CBS Open Data API.

Best practices for CBS Open Data queries:

1. **Dataset discovery**:
   - Start with query_datasets() to find datasets.
   - IMPORTANT: use `filter` with `contains(Title,'...')`
     to search by title.
   - The `search` parameter is unreliable and often returns
     irrelevant results.
   - Example: filter="contains(Title,'warmte')".

2. **Explore a dataset** (optional — only when dimension names or units are unknown):
   - get_dataset_info() -> title, description, status.
   - get_dimensions() -> dimensions with labels.
     NOTE: do NOT use get_dimension_values() -- returns 404 on most
     datasets. get_dimensions() already includes all labels.
   - get_measure_codes() -> measure columns and units.

3. **Fetch observations**:
   - get_observations() -> all obs with auto-pagination.
   - query_observations() -> advanced OData queries.

4. **Filter syntax**:
   - Supported: eq, ne, lt, le, gt, ge, and, or,
     contains().
   - Not supported: has, substringof.

5. **SSL**: verify_ssl=False due to CBS cert issues.

6. **Retries**: max 3 attempts on transient errors.
"""

from __future__ import annotations

import logging
from typing import Any

from mcp.server.fastmcp import FastMCP

from .cbs_open_data_client import (
    CBSOpenDataClient,
    build_filter_expression,
    combine_filters,
)

logger = logging.getLogger(__name__)

DEFAULT_CATALOG = "CBS"
DEFAULT_DATASET_FILTER = "Status ne 'Gediscontinueerd'"
DEFAULT_LIST_LIMIT = 100

mcp = FastMCP(
    name="CBS Open Data",
    instructions=(
        "Use this server to explore CBS Open Data via OData. "
        "Usually start with query_datasets or get_dimensions "
        "before fetching observations."
    ),
)


def _build_dataset_filter(
    filter_value: str | None,
    exclude_discontinued: bool,
) -> str | None:
    """Combine the default dataset filter with an optional
    user-supplied filter.

    Args:
        filter_value: OData filter provided by the user.
        exclude_discontinued: Whether to exclude discontinued datasets
            by default.

    Returns:
        str | None: Combined filter value.
    """

    if not exclude_discontinued:
        return filter_value
    return combine_filters(filter_value, DEFAULT_DATASET_FILTER)


@mcp.tool()
def get_catalogs() -> dict[str, Any]:
    """Fetch all available CBS catalogs.

    Returns:
        dict[str, Any]: Count and list of catalogs.
    """

    with CBSOpenDataClient() as client:
        catalogs = client.get_catalogs()
    return {"count": len(catalogs), "catalogs": catalogs}


@mcp.tool()
def query_datasets(
    catalog: str = DEFAULT_CATALOG,
    select: str | None = None,
    filter: str | None = None,
    orderby: str | None = None,
    top: int = 20,
    skip: int = 0,
    count: bool = True,
    search: str | None = None,
    expand: str | None = None,
    exclude_discontinued: bool = True,
) -> dict[str, Any]:
    """Search datasets with OData filtering, sorting, and pagination.

    RECOMMENDATION: use `filter` with contains() instead of `search`.
    The CBS `$search` often returns irrelevant results.
    Example: filter="contains(Title,'warmte')" or
    filter="contains(Title,'energie') and contains(Title,'verbruik')".

    Args:
        catalog: Catalog identifier, typically `CBS`.
        select: OData `$select`.
        filter: OData `$filter` -- use contains() for title search.
        orderby: OData `$orderby`.
        top: OData `$top`.
        skip: OData `$skip`.
        count: OData `$count`.
        search: OData `$search` (UNRELIABLE -- use filter instead).
        expand: OData `$expand`.
        exclude_discontinued: Prepend a default filter to exclude
            discontinued datasets.

    Returns:
        dict[str, Any]: Query info and dataset results.
    """

    query_options = {
        "select": select,
        "filter": _build_dataset_filter(filter, exclude_discontinued),
        "orderby": orderby,
        "top": max(top, 0),
        "skip": max(skip, 0),
        "count": count,
        "search": search,
        "expand": expand,
    }

    with CBSOpenDataClient() as client:
        result = client.query_datasets(catalog, query_options=query_options)

    return {
        "catalog": catalog,
        "query": query_options,
        "count": len(result["items"]),
        "total_count": result["total_count"],
        "datasets": result["items"],
    }


@mcp.tool()
def get_dimensions(
    catalog: str = DEFAULT_CATALOG,
    dataset: str = "",
) -> dict[str, Any]:
    """Fetch all dimensions for a dataset.

    Args:
        catalog: Catalog identifier.
        dataset: Dataset identifier.

    Returns:
        dict[str, Any]: Table of dimensions and labels per dimension.
    """

    if not dataset:
        raise ValueError("dataset is required")

    with CBSOpenDataClient() as client:
        dimensions = client.get_dimensions(catalog, dataset)
        table_rows: list[dict[str, Any]] = []

        for dimension in dimensions:
            dimension_identifier = str(dimension.get("Identifier", "")).strip()
            if not dimension_identifier:
                continue

            code_values = client.get_dimension_codes(
                catalog,
                dataset,
                dimension_identifier,
            )
            labels = [
                str(code_value.get("Title", "")).strip()
                for code_value in code_values
                if str(code_value.get("Title", "")).strip()
            ]

            table_rows.append(
                {
                    "dimension": dimension_identifier,
                    "labels": labels,
                }
            )

    return {
        "catalog": catalog,
        "dataset": dataset,
        "count": len(table_rows),
        "table": {
            "columns": ["dimension", "labels"],
            "rows": table_rows,
        },
    }


@mcp.tool()
def get_dimension_values(
    catalog: str = DEFAULT_CATALOG,
    dataset: str = "",
    dimension: str = "",
    select: str | None = None,
    filter: str | None = None,
    orderby: str | None = None,
    top: int = 100,
) -> dict[str, Any]:
    """Fetch values for a specific dimension.

    WARNING: this endpoint returns 404 on most CBS datasets.
    Prefer get_dimensions(), which already resolves all dimension
    codes and labels via the reliable {dim}Codes endpoints.

    Args:
        catalog: Catalog identifier.
        dataset: Dataset identifier.
        dimension: Dimension identifier.
        select: OData `$select`.
        filter: Additional OData `$filter`.
        orderby: OData `$orderby`.
        top: OData `$top`.

    Returns:
        dict[str, Any]: Count and dimension values.
    """

    if not dataset:
        raise ValueError("dataset is required")
    if not dimension:
        raise ValueError("dimension is required")

    query_options = {
        "select": select,
        "filter": filter,
        "orderby": orderby,
        "top": max(top, 0),
    }

    with CBSOpenDataClient() as client:
        values = client.get_dimension_values(
            catalog,
            dataset,
            dimension,
            query_options=query_options,
        )

    return {
        "catalog": catalog,
        "dataset": dataset,
        "dimension": dimension,
        "query": query_options,
        "count": len(values),
        "values": values,
    }


@mcp.tool()
def get_observations(
    catalog: str = DEFAULT_CATALOG,
    dataset: str = "",
    filters: dict[str, str] | None = None,
    paginate: bool = True,
    resolve_labels: bool = True,
    limit: int = 10000,
) -> dict[str, Any]:
    """Fetch observations with automatic pagination.

    Automatically follows @odata.nextLink to retrieve all pages.
    With resolve_labels=True, dimension and measure codes are
    replaced with human-readable labels.

    Args:
        catalog: Catalog identifier.
        dataset: Dataset identifier.
        filters: Dimension filters (key=dimension, value=code).
        paginate: Fetch all pages (True) or just one page (False).
        resolve_labels: Replace codes with labels (True) or keep
            raw codes (False, faster).
        limit: Maximum number of observations (safety limit).

    Returns:
        dict[str, Any]: Observation result.
    """

    if not dataset:
        raise ValueError("dataset is required")

    with CBSOpenDataClient() as client:
        if paginate:
            filter_expr = (
                build_filter_expression(filters)
                if filters
                else None
            )
            query_opts = (
                {"filter": filter_expr} if filter_expr else None
            )
            observations = client.get_all_observations(
                catalog, dataset, query_options=query_opts,
            )
        else:
            observations = client.get_observations(
                catalog, dataset, filters=filters,
            )

        safe_limit = max(limit, 0)
        truncated = len(observations) > safe_limit
        visible = observations[:safe_limit]

        if resolve_labels and visible:
            visible = client.resolve_observation_labels(
                catalog, dataset, visible,
            )

    return {
        "catalog": catalog,
        "dataset": dataset,
        "filters": filters or {},
        "total_count": len(observations),
        "returned_count": len(visible),
        "truncated": truncated,
        "resolve_labels": resolve_labels,
        "observations": visible,
    }


@mcp.tool()
def query_observations(
    catalog: str = DEFAULT_CATALOG,
    dataset: str = "",
    select: str | None = None,
    filter: str | None = None,
    orderby: str | None = None,
    top: int = 100,
    skip: int = 0,
    count: bool = True,
    search: str | None = None,
    expand: str | None = None,
) -> dict[str, Any]:
    """Query observations with advanced OData options.

    Args:
        catalog: Catalog identifier.
        dataset: Dataset identifier.
        select: OData `$select`.
        filter: OData `$filter`.
        orderby: OData `$orderby`.
        top: OData `$top`.
        skip: OData `$skip`.
        count: OData `$count`.
        search: OData `$search`.
        expand: OData `$expand`.

    Returns:
        dict[str, Any]: Query info and observation results.
    """

    if not dataset:
        raise ValueError("dataset is required")

    query_options = {
        "select": select,
        "filter": filter,
        "orderby": orderby,
        "top": max(top, 0),
        "skip": max(skip, 0),
        "count": count,
        "search": search,
        "expand": expand,
    }

    with CBSOpenDataClient() as client:
        result = client.query_observations(
            catalog,
            dataset,
            query_options=query_options,
        )

    return {
        "catalog": catalog,
        "dataset": dataset,
        "query": query_options,
        "count": len(result["items"]),
        "total_count": result["total_count"],
        "observations": result["items"],
    }


@mcp.tool()
def get_metadata() -> dict[str, str]:
    """Fetch the OData metadata document (EDM schema).

    Note: This returns the catalog-wide schema, not dataset-specific metadata.
    Use get_dataset_info() for metadata of a single dataset.

    Returns:
        dict[str, str]: Metadata as an XML string.
    """

    with CBSOpenDataClient() as client:
        metadata = client.get_metadata()
    return {"metadata_xml": metadata}


@mcp.tool()
def get_dataset_info(
    catalog: str = DEFAULT_CATALOG,
    dataset: str = "",
) -> dict[str, Any]:
    """Fetch metadata for a single specific dataset.

    Returns title, description, status, last modification date, and more.
    Use this to quickly check whether a dataset is relevant.

    Args:
        catalog: Catalog identifier.
        dataset: Dataset identifier (e.g. '85523NED').

    Returns:
        dict[str, Any]: Dataset metadata.
    """

    if not dataset:
        raise ValueError("dataset is required")

    with CBSOpenDataClient() as client:
        info = client.get_dataset_info(catalog, dataset)
    return {
        "catalog": catalog,
        "dataset": dataset,
        "info": info,
    }


@mcp.tool()
def get_measure_codes(
    catalog: str = DEFAULT_CATALOG,
    dataset: str = "",
) -> dict[str, Any]:
    """Fetch measure code definitions for a dataset.

    Measures are the columns containing actual measurement values in
    observations. Each measure has an Identifier (code) and Title (label).

    Args:
        catalog: Catalog identifier.
        dataset: Dataset identifier.

    Returns:
        dict[str, Any]: Count and list of measure codes.
    """

    if not dataset:
        raise ValueError("dataset is required")

    with CBSOpenDataClient() as client:
        measures = client.get_measure_codes(catalog, dataset)
    return {
        "catalog": catalog,
        "dataset": dataset,
        "count": len(measures),
        "measures": measures,
    }


def main() -> None:
    """Start the MCP server on stdio transport."""

    mcp.run()


if __name__ == "__main__":
    main()
