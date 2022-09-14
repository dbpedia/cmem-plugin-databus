"""Plugin tests."""
from unittest import skip

import pytest

from cmem_plugin_databus.utils import DatabusFileAutocomplete

# def test_plugin():
#     plugin = SimpleDatabusLoadingPlugin(
#         "https://d8lr.tools.dbpedia.org/jlareck/migration/stw-thesaurus-for-economics_2/1.0.10-alpha/stw-thesaurus-for-economics_2.ttl"
#     )
#
#     plugin.execute()


@skip
def test_fetch_results():
    len_mappings = {
        "https://d8lr.tools.dbpedia.org": 2,
        "https://d8lr.tools.dbpedia.org/jlareck/": 3,
        "https://d8lr.tools.dbpedia.org/jlareck/migration/": 4,
        "https://d8lr.tools.dbpedia.org/jlareck/migration/omwn-cow_1/": 5,
        "https://d8lr.tools.dbpedia.org/jlareck/migration/omwn-cow_1/1.0.10-alpha/": 6,
    }

    def assert_correct_result_sizes(databus_uri: str, expected_size: int) -> None:
        results = DatabusFileAutocomplete.fetch_results_by_uri(databus_uri)
        assert len(results) > 0
        for result in results:
            assert len(result.replace("https://", "").split("/")) == expected_size

    for uri, expected_path_size in len_mappings.items():
        assert_correct_result_sizes(uri, expected_path_size)


def test_dummy():
    assert 1 == 1
