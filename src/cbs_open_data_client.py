"""Client for the CBS Open Data OData API."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://datasets.cbs.nl/odata/v1"
DEFAULT_TIMEOUT_SECONDS = 60.0
# CBS API heeft een certificaatprobleem met de standaard CA-bundle
DEFAULT_VERIFY_SSL = False
MAX_RETRIES = 3


def escape_odata_string(value: str) -> str:
    """Escape a string value for safe use in OData filter expressions.

    Args:
        value: Raw text value.

    Returns:
        str: OData-safe string value.
    """

    return value.replace("'", "''")


def normalize_odata_query_options(
    query_options: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Normalize query options to OData parameters.

    Args:
        query_options: Input options with or without `$` prefix.

    Returns:
        dict[str, str]: Normalized query options.
    """

    normalized: dict[str, str] = {}
    if not query_options:
        return normalized

    for key, value in query_options.items():
        if value is None or value == "":
            continue

        normalized_key = key if key.startswith("$") else f"${key}"
        if isinstance(value, bool):
            normalized[normalized_key] = str(value).lower()
        else:
            normalized[normalized_key] = str(value)

    return normalized


def build_filter_expression(filters: Mapping[str, str] | None) -> str | None:
    """Build an OData filter expression from a simple key-value mapping.

    Args:
        filters: Mapping of dimension or column filters.

    Returns:
        str | None: OData filter expression or `None`.
    """

    if not filters:
        return None

    clauses = []
    for key, value in filters.items():
        if value == "":
            continue
        clauses.append(f"{key} eq '{escape_odata_string(value)}'")

    if not clauses:
        return None

    return " and ".join(clauses)


def combine_filters(*filters: str | None) -> str | None:
    """Combine multiple OData filters with `and`.

    Args:
        *filters: One or more filter parts.

    Returns:
        str | None: Combined filter expression or `None`.
    """

    parts = [part.strip() for part in filters if part and part.strip()]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return " and ".join(f"({part})" for part in parts)


def build_dimension_filter(
    dimension: str,
    extra_filter: str | None = None,
) -> str:
    """Build the filter for the `DimensionValues` endpoint.

    Args:
        dimension: Name of the requested dimension.
        extra_filter: Additional filter to apply alongside the
            dimension filter.

    Returns:
        str: Complete filter expression.
    """

    base_filter = f"Dimension eq '{escape_odata_string(dimension)}'"
    return combine_filters(base_filter, extra_filter) or base_filter


class CBSOpenDataClient:
    """Thin client for the CBS Open Data OData API."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        http_client: httpx.Client | None = None,
        verify_ssl: bool = DEFAULT_VERIFY_SSL,
    ) -> None:
        """Initialize the client.

        Args:
            base_url: Base URL of the CBS OData API.
            timeout_seconds: Timeout for HTTP requests in seconds.
            http_client: Optional external `httpx.Client` for testing.
            verify_ssl: Enable SSL verification (disabled by default due to
                CBS certificate issues).
        """

        self.base_url = base_url.rstrip("/")
        self._owns_http_client = http_client is None
        self._verify_ssl = verify_ssl
        self._http_client = http_client or httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=True,
            verify=verify_ssl,
            headers={"User-Agent": "cbs-open-data-mcp/0.1.0"},
        )

    def __enter__(self) -> CBSOpenDataClient:
        """Support context manager usage.

        Returns:
            CBSOpenDataClient: The current client instance.
        """

        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Release HTTP resources on context manager exit.

        Args:
            exc_type: Exception type if any.
            exc: Exception object if any.
            traceback: Traceback if any.
        """

        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client if it was created locally."""

        if self._owns_http_client:
            self._http_client.close()

    def get_catalogs(self) -> list[dict[str, Any]]:
        """Fetch all available catalogs.

        Returns:
            list[dict[str, Any]]: List of catalog objects.
        """

        payload = self._get_json("Catalogs")
        return self._extract_items(payload)

    def get_dimensions(
        self,
        catalog: str,
        dataset: str,
    ) -> list[dict[str, Any]]:
        """Fetch dimensions for a dataset.

        Args:
            catalog: Catalog identifier, typically `CBS`.
            dataset: Dataset identifier.

        Returns:
            list[dict[str, Any]]: List of dimension objects.
        """

        payload = self._get_json(f"{catalog}/{dataset}/Dimensions")
        return self._extract_items(payload)

    def get_dimension_codes(
        self,
        catalog: str,
        dataset: str,
        dimension: str,
    ) -> list[dict[str, Any]]:
        """Fetch code values (labels) for a dimension.

        Args:
            catalog: Catalog identifier, typically `CBS`.
            dataset: Dataset identifier.
            dimension: Dimension identifier.

        Returns:
            list[dict[str, Any]]: List of code objects for the dimension.
        """

        payload = self._get_json(f"{catalog}/{dataset}/{dimension}Codes")
        return self._extract_items(payload)

    def get_dimension_values(
        self,
        catalog: str,
        dataset: str,
        dimension: str,
        query_options: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch values for a dimension.

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.
            dimension: Dimension identifier.
            query_options: Additional OData options.

        Returns:
            list[dict[str, Any]]: List of dimension values.
        """

        normalized_options = normalize_odata_query_options(query_options)
        normalized_options["$filter"] = build_dimension_filter(
            dimension,
            normalized_options.get("$filter"),
        )
        payload = self._get_json(
            f"{catalog}/{dataset}/DimensionValues",
            query_params=normalized_options,
        )
        return self._extract_items(payload)

    def get_observations(
        self,
        catalog: str,
        dataset: str,
        filters: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch observations with key-value filters (single page).

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.
            filters: Simple filter mapping per field.

        Returns:
            list[dict[str, Any]]: List of observations (max 1 page).
        """

        query_options = normalize_odata_query_options(
            {"filter": build_filter_expression(filters)}
        )
        payload = self._get_json(
            f"{catalog}/{dataset}/Observations",
            query_params=query_options,
        )
        return self._extract_items(payload)

    def get_all_observations(
        self,
        catalog: str,
        dataset: str,
        query_options: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all observations with automatic OData pagination.

        Follows `@odata.nextLink` until all pages have been fetched.

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.
            query_options: OData query options (filter, select, etc.).

        Returns:
            list[dict[str, Any]]: All observations.
        """

        params = normalize_odata_query_options(query_options)
        all_items: list[dict[str, Any]] = []
        url: str | None = f"{self.base_url}/{catalog}/{dataset}/Observations"

        while url:
            response = self._http_client.get(
                url,
                params=params if all_items == [] else None,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            all_items.extend(self._extract_items(payload))
            url = payload.get("@odata.nextLink")
            logger.debug(f"Page fetched: {len(all_items)} obs total.")

        return all_items

    def get_measure_codes(
        self,
        catalog: str,
        dataset: str,
    ) -> list[dict[str, Any]]:
        """Fetch measure codes (value labels) for a dataset.

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.

        Returns:
            list[dict[str, Any]]: List of measure code objects.
        """

        payload = self._get_json(f"{catalog}/{dataset}/MeasureCodes")
        return self._extract_items(payload)

    def get_dataset_info(
        self,
        catalog: str,
        dataset: str,
    ) -> dict[str, Any]:
        """Fetch metadata for a single dataset.

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.

        Returns:
            dict[str, Any]: Dataset metadata.
        """

        safe_id = escape_odata_string(dataset)
        payload = self._get_json(
            f"{catalog}/Datasets",
            query_params={
                "$filter": f"Identifier eq '{safe_id}'",
            },
        )
        items = self._extract_items(payload)
        if not items:
            raise RuntimeError(
                f"Dataset '{dataset}' not found in catalog '{catalog}'."
            )
        return items[0]

    def resolve_observation_labels(
        self,
        catalog: str,
        dataset: str,
        observations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Replace dimension and measure codes with human-readable labels.

        Fetches all dimension codes and measure codes once, then replaces
        coded values in observations with labels.

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.
            observations: List of observations (with codes).

        Returns:
            list[dict[str, Any]]: Observations with labels.
        """
        if not observations:
            return observations

        # Fetch dimensions and build code→label mappings
        dimensions = self.get_dimensions(catalog, dataset)
        dim_maps: dict[str, dict[str, str]] = {}
        for dim in dimensions:
            dim_id = str(dim.get("Identifier", "")).strip()
            if not dim_id:
                continue
            codes = self.get_dimension_codes(
                catalog, dataset, dim_id,
            )
            dim_maps[dim_id] = {
                c["Identifier"]: c.get("Title", c["Identifier"])
                for c in codes
                if "Identifier" in c
            }

        # Fetch measure codes
        measures = self.get_measure_codes(catalog, dataset)
        measure_map = {
            m["Identifier"]: m.get("Title", m["Identifier"])
            for m in measures
            if "Identifier" in m
        }

        # Apply labels to observations
        labeled: list[dict[str, Any]] = []
        for obs in observations:
            row = dict(obs)
            for dim_id, code_map in dim_maps.items():
                if dim_id in row:
                    raw = row[dim_id]
                    row[dim_id] = code_map.get(raw, raw)
            if "Measure" in row:
                raw = row["Measure"]
                row["Measure"] = measure_map.get(raw, raw)
            labeled.append(row)

        return labeled

    def query_datasets(
        self,
        catalog: str,
        query_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute an OData query on datasets.

        Args:
            catalog: Catalog identifier.
            query_options: OData query options.

        Returns:
            dict[str, Any]: Object with `items` and optional `total_count`.
        """

        payload = self._get_json(
            f"{catalog}/Datasets",
            query_params=normalize_odata_query_options(query_options),
        )
        return {
            "items": self._extract_items(payload),
            "total_count": self._extract_count(payload),
        }

    def query_observations(
        self,
        catalog: str,
        dataset: str,
        query_options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute an OData query on observations.

        Args:
            catalog: Catalog identifier.
            dataset: Dataset identifier.
            query_options: OData query options.

        Returns:
            dict[str, Any]: Object with `items` and optional `total_count`.
        """

        payload = self._get_json(
            f"{catalog}/{dataset}/Observations",
            query_params=normalize_odata_query_options(query_options),
        )
        return {
            "items": self._extract_items(payload),
            "total_count": self._extract_count(payload),
        }

    def get_metadata(self) -> str:
        """Fetch the OData metadata document.

        Returns:
            str: XML content of the metadata document.
        """

        response = self._request("/$metadata", accept="application/xml")
        return response.text

    def _get_json(
        self,
        path: str,
        query_params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a JSON request to the CBS API.

        Args:
            path: Relative endpoint path.
            query_params: Query parameters for the request.

        Returns:
            dict[str, Any]: Decoded JSON payload.
        """

        response = self._request(
            path,
            query_params=query_params,
            accept="application/json",
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("CBS API returned invalid JSON.") from exc

        if not isinstance(payload, dict):
            raise RuntimeError("CBS API returned an unexpected payload type.")
        return payload

    def _request(
        self,
        path: str,
        query_params: Mapping[str, Any] | None = None,
        accept: str = "application/json",
    ) -> httpx.Response:
        """Execute an HTTP GET request with retry logic and error handling.

        Args:
            path: Relative endpoint path.
            query_params: Query parameters for the request.
            accept: Desired `Accept` header format.

        Returns:
            httpx.Response: Successful HTTP response.

        Raises:
            RuntimeError: If the API request fails persistently.
        """

        url = f"{self.base_url}/{path.lstrip('/')}"
        last_exception: httpx.HTTPError | None = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                logger.debug(
                    f"CBS request: {url} (attempt {attempt + 1}/{MAX_RETRIES + 1})"
                )
                response = self._http_client.get(
                    url,
                    params=query_params,
                    headers={"Accept": accept},
                )
                response.raise_for_status()
                logger.debug(f"CBS request succeeded: {response.status_code}")
                return response

            except httpx.HTTPStatusError as exc:
                last_exception = exc
                body_preview = exc.response.text[:200]
                logger.warning(
                    f"CBS API status {exc.response.status_code} "
                    f"(attempt {attempt + 1}): {body_preview}"
                )
                # Don't retry on 4xx errors except 429 (rate limit)
                if (
                    400 <= exc.response.status_code < 500
                    and exc.response.status_code != 429
                ):
                    break

            except httpx.HTTPError as exc:
                last_exception = exc
                logger.warning(f"CBS request failed (attempt {attempt + 1}): {exc}")

        # All retries exhausted
        if isinstance(last_exception, httpx.HTTPStatusError):
            body_preview = last_exception.response.text[:500]
            msg = (
                f"CBS API request failed after {MAX_RETRIES + 1} attempts "
                f"with status {last_exception.response.status_code}: {body_preview}"
            )
        else:
            attempts = MAX_RETRIES + 1
            msg = f"CBS API request failed after {attempts} attempts: {last_exception}"

        logger.error(msg)
        raise RuntimeError(msg) from last_exception

    @staticmethod
    def _extract_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Extract `value` list from an OData payload.

        Args:
            payload: JSON payload from the CBS API.

        Returns:
            list[dict[str, Any]]: List of objects.
        """

        value = payload.get("value", [])
        if not isinstance(value, list):
            raise RuntimeError("CBS API returned an invalid `value` list.")
        items: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                items.append(dict(item))
        return items

    @staticmethod
    def _extract_count(payload: Mapping[str, Any]) -> int | None:
        """Extract the optional OData count from a payload.

        Args:
            payload: JSON payload from the CBS API.

        Returns:
            int | None: Total count if available.
        """

        raw_count = payload.get("@odata.count")
        if raw_count is None:
            return None
        try:
            return int(raw_count)
        except (TypeError, ValueError):
            return None
