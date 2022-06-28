from typing import Dict, List, Optional
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import URLError
from cmem_plugin_base.dataintegration.types import StringParameterType, Autocompletion


def fetch_query_result_by_key(endpoint: str, query: str, key: str) -> Optional[List[str]]:
    sparql_service = SPARQLWrapper(endpoint)
    sparql_service.setQuery(query)
    sparql_service.setReturnFormat(JSON)

    try:
        query_results = sparql_service.query().convert()
    except URLError:
        return None
    try:
        results = list(map(lambda binding: binding[key]["value"], query_results["results"]["bindings"]))
    except KeyError:
        results = []

    return results


def load_accounts(sparql_endpoint: str) -> List[str]:
    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT DISTINCT ?acc WHERE {
  ?acc a foaf:PersonalProfileDocument .
} """

    return fetch_query_result_by_key(sparql_endpoint, query, "acc")


def load_groups(sparql_endpoint: str, publisher_uri: str) -> List[str]:
    query = ("PREFIX dct: <http://purl.org/dc/terms/>\n" +
             "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
             "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
             "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
             "SELECT DISTINCT ?group WHERE {\n" +
             "?dataset a dataid:Dataset .\n" +
             f"?dataset dct:publisher <{publisher_uri}> .\n"
             "?dataset dataid:group ?group .}")
    return fetch_query_result_by_key(sparql_endpoint, query, "group")


def load_artifacts(sparql_endpoint: str, group_id: str) -> List[str]:
    query = ("PREFIX dct: <http://purl.org/dc/terms/>\n" +
             "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
             "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
             "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
             "SELECT DISTINCT ?artifact WHERE {\n" +
             f"?dataset dataid:group <{group_id}> .\n" +
             "?dataset dataid:artifact ?artifact .}")

    return fetch_query_result_by_key(sparql_endpoint, query, "artifact")


def load_versions(sparql_endpoint: str, artifact_id: str) -> List[str]:
    query = ("PREFIX dct: <http://purl.org/dc/terms/>\n" +
             "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
             "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
             "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
             "SELECT DISTINCT ?version WHERE {\n" +
             f"?dataset dataid:artifact <{artifact_id}> .\n" +
             "?dataset dataid:version ?version .}")

    return fetch_query_result_by_key(sparql_endpoint, query, "version")


def load_files(sparql_endpoint: str, version_id: str) -> List[str]:
    query = ("PREFIX dct: <http://purl.org/dc/terms/>\n" +
             "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
             "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
             "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n" +
             "PREFIX dcat: <http://www.w3.org/ns/dcat#>\n" +
             "SELECT DISTINCT ?file WHERE {\n" +
             f"?dataset dataid:version <{version_id}> .\n" +
             "?dataset dcat:distribution ?dist .\n" +
             "?dist dataid:file ?file. }")
    print(query)
    return fetch_query_result_by_key(sparql_endpoint, query, "file")


class DatabusFileAutocomplete(StringParameterType):

    def autocomplete(
            self, query_terms: list[str], project_id: Optional[str] = None
    ) -> list[Autocompletion]:
        return self.__transform_uris_to_autocompletion(self.fetch_results(query_terms[0]))

    @staticmethod
    def fetch_results(query_str: str):

        query_no_http = query_str.replace("https://", "")
        parts = query_no_http.rstrip("/ ").rsplit("/", 5)

        endpoint = "https://" + parts[0] + "/sparql"

        normalized_querystr = query_str.rstrip("/ ")

        if len(parts) == 1:
            # when its only the databus -> fetch possible accounts
            return load_accounts(endpoint)
        elif len(parts) == 2:
            # when its the account return groups
            # needs #this appended for publisher id
            return load_groups(endpoint, normalized_querystr + "#this")
        elif len(parts) == 3:
            # when its a group -> return artifacts
            return load_artifacts(endpoint, normalized_querystr)
        elif len(parts) == 4:
            # when its a artifact -> return versions
            return load_versions(endpoint, normalized_querystr)
        elif len(parts) == 5:
            # when its a version -> return files
            return load_files(endpoint, normalized_querystr)

    @staticmethod
    def __transform_uris_to_autocompletion(uri_list: List[str]) -> List[Autocompletion]:

        result = [Autocompletion(uri, uri) for uri in uri_list]
        return result
