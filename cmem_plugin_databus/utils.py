"""Utils for handling the DBpedia Databus"""

from typing import List, Optional, Dict, Generator, Iterator
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import quote
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport

import requests
from SPARQLWrapper import SPARQLWrapper, JSON
from cmem_plugin_base.dataintegration.types import StringParameterType, Autocompletion
from cmem.cmempy.dp.proxy.graph import _get_graph_uri as cmem_get_graph_uri
from cmem.cmempy.api import request


class WebDAVException(Exception):
    def __init__(self, resp: requests.Response):
        super().__init__(
            f"Exception during WebDAV Request {resp.request.method} to {resp.request.url}: Status {resp.status_code}\nResponse: {resp.text}"
        )


def post_streamed_bytes(
    graph: str,
    data: Iterator[bytes],
    endpoint_id="default",
    replace=False,
    content_type="text/turtle",
):
    """Upload graph (streamed).

    Add the content of triple to a remote graph or replace the remote graph
    with the content of a triple file.

    Args:
        graph (str): The URI of the remote graph.
        data: content as BytesIO
        endpoint_id (str): Optional endpoint ID (always 'default').
        replace (bool): add (False) or replace (True)
        content_type (str): mime type of the file to post (default is turtle)

    Returns:
        requests.Response object

    """
    uri = cmem_get_graph_uri(endpoint_id, graph) + "&replace=" + str(replace).lower()
    headers = {"Content-Type": content_type}
    # https://2.python-requests.org/en/master/user/advanced/#streaming-uploads
    response = request(uri, method="POST", headers=headers, data=data, stream=True)
    return response


def get_clock(counter: int) -> str:
    """returns a clock symbol"""
    clock = {
        0: "🕛",
        1: "🕐",
        2: "🕑",
        3: "🕓",
        4: "🕔",
        5: "🕕",
        6: "🕖",
        7: "🕗",
        8: "🕘",
        9: "🕚",
    }
    return clock[int(repr(counter)[-1])]


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


def fetch_query_result_by_key(endpoint: str, query: str, key: str) -> List[str]:
    """Sends a query to the given endpint and collects all results of a key in a list"""
    sparql_service = SPARQLWrapper(endpoint)
    sparql_service.setQuery(query)
    sparql_service.setReturnFormat(JSON)

    query_results = sparql_service.query().convert()

    # just to make mypy stop complaining
    assert isinstance(query_results, dict)

    try:
        results = list(
            map(
                lambda binding: str(binding[key]["value"]),
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

        normalized_querystr = query_str[0 : query_str.rfind("/")]

        try:
            if len(parts) == 1:
                # when it's only the databus -> fetch possible accounts
                return load_accounts(endpoint)
            if len(parts) == 2:
                # when it's the account return groups
                # needs #this appended for publisher id
                return load_groups(endpoint, normalized_querystr + "#this")
            if len(parts) == 3:
                # when it's a group -> return artifacts
                return load_artifacts(endpoint, normalized_querystr)
            if len(parts) == 4:
                # when it's an artifact -> return versions
                return load_versions(endpoint, normalized_querystr)
            if len(parts) == 5:
                # when it's a version -> return files
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

        if resp.status_code == 405:
            return True
        else:
            return False

    def create_dir(
        self, path: str, session: requests.Session = None
    ) -> requests.Response:

        if session is None:
            session = requests.Session()

        req = requests.Request(
            method="MKCOL",
            url=f"{self.dav_base}{path}",
            headers={"X-API-KEY": f"{self.api_key}"},
        )
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
                if resp.status_code not in [200, 201, 405]:
                    raise WebDAVException(resp)

        return responses

    def upload_file_with_context(
        self,
        path: str,
        data: bytes,
        context: ExecutionContext,
        chunk_size: int,
        create_parent_dirs: bool = False,
    ) -> requests.Response:

        context_data_generator = byte_iterator_context_update(
            data, context, op_desc="Uploading File", chunksize=chunk_size
        )

        if create_parent_dirs:
            dirpath, filename = path.rsplit("/", 1)
            responses = self.create_dirs(dirpath)
            # when list not empty (=> every dir existed) and last one was an error raise exception
            if responses and responses[-1].status_code not in [200, 201, 405]:
                raise WebDAVException(responses[-1])

        resp = requests.put(
            url=f"{self.dav_base}{path}",
            headers={"X-API-KEY": f"{self.api_key}"},
            data=context_data_generator,
        )

        return resp

    def upload_file(
        self, path: str, data: bytes, create_parent_dirs: bool = False
    ) -> requests.Response:

        if create_parent_dirs:
            dirpath, filename = path.rsplit("/", 1)
            _ = self.create_dirs(dirpath)

        resp = requests.put(
            url=f"{self.dav_base}{path}",
            headers={"X-API-KEY": f"{self.api_key}"},
            data=data,
        )

        return resp


def byte_iterator_context_update(
    data: bytes, context: ExecutionContext, chunksize: int, desc: str
) -> Iterator[bytes]:
    for i, chunk in enumerate(
        [data[i : i + chunksize] for i in range(0, len(data), chunksize)]
    ):
        op_desc = f"{desc} {get_clock(i)}"
        context.report.update(
            ExecutionReport(
                entity_count=(i * chunksize) // 1000000,
                operation="wait",
                operation_desc=op_desc,
            )
        )
        yield chunk
