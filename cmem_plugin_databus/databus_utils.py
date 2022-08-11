"""Utils for handling the DBpedia Databus"""

from typing import List, Optional, Dict
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import quote

import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from cmem_plugin_base.dataintegration.types import StringParameterType, Autocompletion


class WebDAVException(Exception):

    def __init__(self, resp: requests.Response):
        super().__init__(f"Exception during WebDAV Request {resp.request.method} to {resp.request.url}: {resp.text}")


@dataclass
class DatabusSearchResult:
    typeName: str
    score: float
    label: str
    resource: str


def result_from_json_dict(json_dict: Dict[str, List[str]]) -> DatabusSearchResult:
    return DatabusSearchResult(
        json_dict["typeName"][0],
        float(json_dict["score"][0]),
        json_dict["label"][0],
        json_dict["resource"][0],
    )


def fetch_api_search_result(
        databus_base: str, query_str: str
) -> List[DatabusSearchResult]:
    encoded_query_str = quote(query_str)

    request_uri = f"{databus_base}/api/search?query={encoded_query_str}"

    json_resp = requests.get(request_uri).json()

    result = []

    for res in json_resp["docs"]:
        result.append(result_from_json_dict(res))

    return result


def fetch_query_result_by_key(
        endpoint: str, query: str, key: str
) -> Optional[List[str]]:
    """Sends a query to the given endpint and collects all results of a key in a list"""
    sparql_service = SPARQLWrapper(endpoint)
    sparql_service.setQuery(query)
    sparql_service.setReturnFormat(JSON)

    try:
        query_results = sparql_service.query().convert()
    except URLError:
        return None
    try:
        results = list(
            map(
                lambda binding: binding[key]["value"],
                query_results["results"]["bindings"],
            )
        )
    except KeyError:
        results = []

    return results


def load_accounts(sparql_endpoint: str) -> List[str]:
    """Load available publishers from the databus.
    Only accounts, not all publishers defined"""
    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT DISTINCT ?acc WHERE {
  ?acc a foaf:PersonalProfileDocument .
} """

    return fetch_query_result_by_key(sparql_endpoint, query, "acc")


def load_groups(sparql_endpoint: str, publisher_uri: str) -> List[str]:
    """Load groups for a given publisher ID. CARE: #this is expected at the end!"""
    query = (
            "PREFIX dct: <http://purl.org/dc/terms/>\n"
            + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
            + "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n"
            + "SELECT DISTINCT ?group WHERE {\n"
            + "?dataset a dataid:Dataset .\n"
            + f"?dataset dct:publisher <{publisher_uri}> .\n"
              "?dataset dataid:group ?group .}"
    )
    return fetch_query_result_by_key(sparql_endpoint, query, "group")


def load_artifacts(sparql_endpoint: str, group_id: str) -> List[str]:
    """Load artifacts for a given group ID"""
    query = (
            "PREFIX dct: <http://purl.org/dc/terms/>\n"
            + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
            + "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n"
            + "SELECT DISTINCT ?artifact WHERE {\n"
            + f"?dataset dataid:group <{group_id}> .\n"
            + "?dataset dataid:artifact ?artifact .}"
    )

    return fetch_query_result_by_key(sparql_endpoint, query, "artifact")


def load_versions(sparql_endpoint: str, artifact_id: str) -> List[str]:
    """Load versions for a given artifact ID"""
    query = (
            "PREFIX dct: <http://purl.org/dc/terms/>\n"
            + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
            + "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n"
            + "SELECT DISTINCT ?version WHERE {\n"
            + f"?dataset dataid:artifact <{artifact_id}> .\n"
            + "?dataset dataid:version ?version .}"
    )

    return fetch_query_result_by_key(sparql_endpoint, query, "version")


def load_files(sparql_endpoint: str, version_id: str) -> List[str]:
    """Load files for a given version ID"""
    query = (
            "PREFIX dct: <http://purl.org/dc/terms/>\n"
            + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
            + "PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>\n"
            + "PREFIX dcat: <http://www.w3.org/ns/dcat#>\n"
            + "SELECT DISTINCT ?file WHERE {\n"
            + f"?dataset dataid:version <{version_id}> .\n"
            + "?dataset dcat:distribution ?dist .\n"
            + "?dist dataid:file ?file. }"
    )
    return fetch_query_result_by_key(sparql_endpoint, query, "file")


class DatabusFileAutocomplete(StringParameterType):
    """Class for autocompleting identifiers from an arbitrary databus"""

    def autocomplete(
            self, query_terms: list[str], project_id: Optional[str] = None
    ) -> list[Autocompletion]:
        return self.__transform_uris_to_autocompletion(
            self.fetch_results_by_uri(query_terms[0])
        )

    @staticmethod
    def fetch_results_by_uri(query_str: str) -> List[str]:
        """Fetches results for autocompletion for Databus File Identifiers and returns a list of URIs"""

        query_no_http = query_str.replace("https://", "")
        parts = query_no_http.rstrip("/ ").rsplit("/", 5)

        endpoint = "https://" + parts[0] + "/sparql"

        normalized_querystr = query_str[0: query_str.rfind("/")]

        try:
            if len(parts) == 1:
                # when its only the databus -> fetch possible accounts
                return load_accounts(endpoint)
            if len(parts) == 2:
                # when its the account return groups
                # needs #this appended for publisher id
                return load_groups(endpoint, normalized_querystr + "#this")
            if len(parts) == 3:
                # when its a group -> return artifacts
                return load_artifacts(endpoint, normalized_querystr)
            if len(parts) == 4:
                # when its a artifact -> return versions
                return load_versions(endpoint, normalized_querystr)
            if len(parts) == 5:
                # when its a version -> return files
                return load_files(endpoint, normalized_querystr)
        except Exception:
            pass

        return [query_str]

    @staticmethod
    def __transform_uris_to_autocompletion(uri_list: List[str]) -> List[Autocompletion]:
        """transforms a list of URIs into a list of autocompletion"""
        result = [Autocompletion(uri, uri) for uri in uri_list]
        return result


class WebDAVHandler:

    def __init__(self, databus_base: str, user: str, api_key: str):
        self.dav_base = databus_base + f"dav/{user}/"
        self.api_key = api_key

    def check_existence(self, path: str) -> bool:
        try:
            resp = requests.head(url=f"{self.dav_base}{path}")
        except requests.RequestException:
            return False

        if resp.status_code == 200:
            return True
        else:
            return False

    def create_dir(self, path: str) -> requests.Response:
        req = requests.Request(method="MKCOL", url=f"{self.dav_base}{path}", headers={"X-API-KEY": f"{self.api_key}"})

        session = requests.Session()

        resp = session.send(req.prepare())

        return resp

    def create_dirs(self, path: str) -> List[requests.Response]:

        dirs = path.split("/")
        responses = []
        current_path = ""
        for directory in dirs:
            current_path = current_path + directory + "/"
            if not self.check_existence(current_path):
                resp = self.create_dir(current_path)
                responses.append(resp)
                if resp.status_code >= 400:
                    break

        return responses

    def upload_file(self, path: str, data: bytes, create_parent_dirs: bool = False) -> requests.Response:

        if create_parent_dirs:
            dirpath, filename = path.rsplit("/", 1)
            responses = self.create_dirs(dirpath)

            if responses[-1].status_code >= 400:
                raise WebDAVException(responses[-1])

        resp = requests.put(url=f"{self.dav_base}{path}", data=data)

        return resp
