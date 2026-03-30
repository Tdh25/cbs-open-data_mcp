"""Tests voor de CBS Open Data client.

Gebruik:
    uv run python -m unittest tests.test_cbs_open_data_client
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from src.cbs_open_data_client import (
    CBSOpenDataClient,
    build_dimension_filter,
    build_filter_expression,
    combine_filters,
    normalize_odata_query_options,
)


class TestCBSOpenDataHelpers(unittest.TestCase):
    """Unit tests voor query- en filterhelpers."""

    def test_normalize_odata_query_options_prefixes_and_formats_values(
        self,
    ) -> None:
        """Controleer dat query-opties het juiste OData-formaat krijgen."""

        result = normalize_odata_query_options(
            {"top": 10, "$skip": 5, "count": True, "search": "gas"}
        )

        self.assertEqual(
            result,
            {"$top": "10", "$skip": "5", "$count": "true", "$search": "gas"},
        )

    def test_build_filter_expression_escapes_quotes(self) -> None:
        """Controleer dat eenvoudige filters correct en veilig werken."""

        result = build_filter_expression({"Perioden": "2024JJ00", "RegioS": "'NL01'"})

        self.assertEqual(
            result,
            "Perioden eq '2024JJ00' and RegioS eq '''NL01'''",
        )

    def test_combine_filters_wraps_multiple_parts(self) -> None:
        """Controleer dat meerdere filterdelen veilig gecombineerd worden."""

        result = combine_filters(
            "Status ne 'Gediscontinueerd'",
            "Modified gt 2024-01-01",
        )

        self.assertEqual(
            result,
            "(Status ne 'Gediscontinueerd') and (Modified gt 2024-01-01)",
        )

    def test_build_dimension_filter_adds_dimension_constraint(self) -> None:
        """Controleer dat het dimensiefilter altijd aanwezig blijft."""

        result = build_dimension_filter("RegioS", "Title ne null")

        self.assertEqual(result, "(Dimension eq 'RegioS') and (Title ne null)")


class TestCBSOpenDataClient(unittest.TestCase):
    """Unit tests voor HTTP-response parsing."""

    def test_query_datasets_returns_items_and_count(self) -> None:
        """Controleer dat datasetquery's items en `@odata.count` teruggeven."""

        response = MagicMock()
        response.json.return_value = {
            "value": [{"identifier": "123", "title": "Demo dataset"}],
            "@odata.count": 42,
        }
        response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.return_value = response

        client = CBSOpenDataClient(http_client=http_client)
        result = client.query_datasets(
            "CBS",
            query_options={"top": 1, "count": True},
        )

        self.assertEqual(result["total_count"], 42)
        self.assertEqual(len(result["items"]), 1)

    def test_get_metadata_returns_xml_text(self) -> None:
        """Controleer dat metadata als XML-string wordt teruggegeven."""

        response = MagicMock()
        response.text = "<xml>metadata</xml>"
        response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.return_value = response

        client = CBSOpenDataClient(http_client=http_client)
        result = client.get_metadata()

        self.assertEqual(result, "<xml>metadata</xml>")

    def test_get_dimension_codes_returns_items(self) -> None:
        """Controleer dat `get_dimension_codes` code-items teruggeeft."""

        response = MagicMock()
        response.json.return_value = {
            "value": [{"Identifier": "X1", "Title": "Label X1"}],
        }
        response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.return_value = response

        client = CBSOpenDataClient(http_client=http_client)
        result = client.get_dimension_codes("CBS", "85523NED", "Sector")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Title"], "Label X1")

    def test_get_measure_codes_returns_items(self) -> None:
        """Controleer dat `get_measure_codes` measure-items teruggeeft."""

        response = MagicMock()
        response.json.return_value = {
            "value": [
                {"Identifier": "M001", "Title": "Warmteproductie"},
                {"Identifier": "M002", "Title": "Aantal installaties"},
            ],
        }
        response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.return_value = response

        client = CBSOpenDataClient(http_client=http_client)
        result = client.get_measure_codes("CBS", "85523NED")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["Title"], "Warmteproductie")

    def test_get_dataset_info_returns_single_dataset(self) -> None:
        """Controleer dat get_dataset_info metadata teruggeeft."""

        response = MagicMock()
        response.json.return_value = {
            "value": [
                {
                    "Identifier": "85523NED",
                    "Title": "Warmtepompen; aantallen",
                    "Status": "Regulier",
                }
            ],
        }
        response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.return_value = response

        client = CBSOpenDataClient(http_client=http_client)
        result = client.get_dataset_info("CBS", "85523NED")

        self.assertEqual(result["Identifier"], "85523NED")
        self.assertEqual(result["Title"], "Warmtepompen; aantallen")

    def test_get_dataset_info_raises_on_not_found(self) -> None:
        """Controleer dat get_dataset_info fout geeft bij ontbrekende dataset."""

        response = MagicMock()
        response.json.return_value = {"value": []}
        response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.return_value = response

        client = CBSOpenDataClient(http_client=http_client)
        with self.assertRaises(RuntimeError):
            client.get_dataset_info("CBS", "ONBEKEND")

    def test_get_all_observations_follows_pagination(self) -> None:
        """Controleer dat `get_all_observations` @odata.nextLink volgt."""

        page1_response = MagicMock()
        page1_response.json.return_value = {
            "value": [{"Id": 0, "Value": 100}],
            "@odata.nextLink": "https://example.com/page2",
        }
        page1_response.raise_for_status.return_value = None

        page2_response = MagicMock()
        page2_response.json.return_value = {
            "value": [{"Id": 1, "Value": 200}],
        }
        page2_response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.side_effect = [page1_response, page2_response]

        client = CBSOpenDataClient(http_client=http_client)
        result = client.get_all_observations("CBS", "85523NED")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["Value"], 100)
        self.assertEqual(result[1]["Value"], 200)
        self.assertEqual(http_client.get.call_count, 2)

    def test_resolve_observation_labels_replaces_codes(
        self,
    ) -> None:
        """Controleer dat codes vervangen worden door labels."""

        # Dimensions response
        dims_response = MagicMock()
        dims_response.json.return_value = {
            "value": [{"Identifier": "Sector"}],
        }
        dims_response.raise_for_status.return_value = None

        # Sector codes response
        sector_codes_response = MagicMock()
        sector_codes_response.json.return_value = {
            "value": [
                {"Identifier": "S1", "Title": "Woningen"},
                {"Identifier": "S2", "Title": "Utiliteit"},
            ],
        }
        sector_codes_response.raise_for_status.return_value = None

        # Measure codes response
        measure_codes_response = MagicMock()
        measure_codes_response.json.return_value = {
            "value": [
                {"Identifier": "M1", "Title": "Warmteproductie"},
            ],
        }
        measure_codes_response.raise_for_status.return_value = None

        http_client = MagicMock()
        http_client.get.side_effect = [
            dims_response,
            sector_codes_response,
            measure_codes_response,
        ]

        client = CBSOpenDataClient(http_client=http_client)
        observations = [
            {"Sector": "S1", "Measure": "M1", "Value": 42},
            {"Sector": "S2", "Measure": "M1", "Value": 99},
        ]

        result = client.resolve_observation_labels(
            "CBS",
            "85523NED",
            observations,
        )

        self.assertEqual(result[0]["Sector"], "Woningen")
        self.assertEqual(result[1]["Sector"], "Utiliteit")
        self.assertEqual(result[0]["Measure"], "Warmteproductie")

    def test_resolve_observation_labels_empty_list(
        self,
    ) -> None:
        """Controleer dat lege lijst direct terugkomt zonder API-calls."""

        http_client = MagicMock()
        client = CBSOpenDataClient(http_client=http_client)
        result = client.resolve_observation_labels(
            "CBS",
            "85523NED",
            [],
        )

        self.assertEqual(result, [])
        http_client.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
