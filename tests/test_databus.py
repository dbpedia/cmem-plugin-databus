"""Plugin tests."""
from cmem_plugin_databus.loader import SimpleDatabusLoadingPlugin
from cmem_plugin_databus.databus_utils import *


# def test_execution():
#     """Test plugin execution"""
#     test_loading_accs()


# def test_loading_accs():
#     accs = load_accounts("https://d8lr.tools.dbpedia.org/sparql")
#     assert len(accs) > 0
#     for acc in accs:
#         assert len(acc.replace("https://", "").split("/")) == 2
#
#
# def test_loading_groups():
#     groups = load_groups("https://d8lr.tools.dbpedia.org/sparql", "https://d8lr.tools.dbpedia.org/jlareck#this")
#     assert len(groups) > 0
#     for group in groups:
#         assert len(group.replace("https://", "").split("/")) == 3
#
#
# def test_loading_artifacts():
#     artifacts = load_artifacts("https://d8lr.tools.dbpedia.org/sparql",
#                                "https://d8lr.tools.dbpedia.org/jlareck/migration")
#     assert len(artifacts) > 0
#     for art in artifacts:
#         assert len(art.replace("https://", "").split("/")) == 4
#
#
# def test_loading_versions():
#     versions = load_versions("https://d8lr.tools.dbpedia.org/sparql",
#                              "https://d8lr.tools.dbpedia.org/jlareck/migration/omwn-cow_1")
#     assert len(versions) > 0
#     for vers in versions:
#         assert len(vers.replace("https://", "").split("/")) == 5
#
#
# def test_loading_files():
#     files = load_artifacts("https://d8lr.tools.dbpedia.org/sparql",
#                            "https://d8lr.tools.dbpedia.org/jlareck/migration/omwn-cow_1/1.0.10-alpha")
#     assert len(files) > 0
#     for f in files:
#         assert len(f.replace("https://", "").split("/")) == 6

def test_fetch_results():

    accs = DatabusFileAutocomplete.fetch_results("https://d8lr.tools.dbpedia.org")
    assert len(accs) > 0
    for acc in accs:
        assert len(acc.replace("https://", "").split("/")) == 2

    groups = DatabusFileAutocomplete.fetch_results("https://d8lr.tools.dbpedia.org/jlareck/")
    for group in groups:
        assert len(group.replace("https://", "").split("/")) == 3

    artifacts = DatabusFileAutocomplete.fetch_results("https://d8lr.tools.dbpedia.org/jlareck/migration/")
    assert len(artifacts) > 0
    for art in artifacts:
        assert len(art.replace("https://", "").split("/")) == 4

    versions = DatabusFileAutocomplete.fetch_results("https://d8lr.tools.dbpedia.org/jlareck/migration/omwn-cow_1/")
    assert len(versions) > 0
    for vers in versions:
        assert len(vers.replace("https://", "").split("/")) == 5

    files = DatabusFileAutocomplete.fetch_results("https://d8lr.tools.dbpedia.org/jlareck/migration/omwn-cow_1/1.0.10-alpha/")
    assert len(files) > 0
    for f in files:
        assert len(f.replace("https://", "").split("/")) == 6