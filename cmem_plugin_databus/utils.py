"""Utils for handling the DBpedia Databus"""
import http
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    PluginContext,
)
from cmem_plugin_base.dataintegration.types import Autocompletion, StringParameterType
from requests import RequestException
from SPARQLWrapper import JSON, SPARQLWrapper

USED_PREFIXES = """
PREFIX databus: <https://dataid.dbpedia.org/databus#>
PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dcv: <https://dataid.dbpedia.org/databus-cv#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


class WebDAVError(Exception):
    """Generalized exception for WebDAV requests"""

    def __init__(self, resp: requests.Response):
        super().__init__(
            f"Exception during WebDAV Request {resp.request.method} to "
            f"{resp.request.url}: Status {resp.status_code}\nResponse: {resp.text}"
        )


class MissingMetadataError(Exception):
    """Exception for missing metadata labels from a given Source"""

    def __init__(self, source: str, metadata_label: str):
        super().__init__(
            f"Exeption during Metadata access from {source}: "
            f"{metadata_label} could not be fetched or is empty"
        )


def get_clock(counter: int) -> str:
    """Return a clock symbol"""
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
    """Databus Search Result"""

    type_name: str
    score: float
    label: str
    resource: str


def result_from_json_dict(json_dict: dict[str, list[str]]) -> DatabusSearchResult:
    """Create a DatabusSearchResult from a json_dict"""
    return DatabusSearchResult(
        json_dict["typeName"][0],
        float(json_dict["score"][0]),
        json_dict["label"][0],
        json_dict["resource"][0],
    )


def fetch_api_search_result(
    databus_base: str, url_parameters: dict | None = None
) -> list[DatabusSearchResult]:
    """Fetch Search Results."""
    encoded_query_str = ""
    if url_parameters:
        encoded_query_str = urlencode(url_parameters)
    request_uri = f"{databus_base}/api/search?{encoded_query_str}"
    json_resp = requests.get(request_uri, timeout=30).json()
    return [result_from_json_dict(res) for res in json_resp["docs"]]


def fetch_query_result_by_key(endpoint: str, query: str, key: str) -> list[str]:
    """Send a query to the given endpoint and collect all results of a key in a list"""
    sparql_service = SPARQLWrapper(endpoint)
    sparql_service.setQuery(query)
    sparql_service.setReturnFormat(JSON)

    query_results = sparql_service.query().convert()

    # just to make mypy stop complaining
    assert isinstance(query_results, dict)  # nosec

    try:
        results = [str(binding[key]["value"]) for binding in query_results["results"]["bindings"]]
    except KeyError:
        results = []

    return results


def load_accounts(sparql_endpoint: str) -> list[str]:
    """Load available publishers from the databus.

    Only accounts, not all publishers defined
    """
    query = f"""{USED_PREFIXES}
    SELECT DISTINCT ?acc WHERE {{
        ?acc a foaf:PersonalProfileDocument .
    }}"""
    return fetch_query_result_by_key(sparql_endpoint, query, "acc")


def load_groups(sparql_endpoint: str, publisher_uri: str) -> list[str]:
    """Load groups for a given publisher ID. CARE: #this is expected at the end!"""
    query = f"""{USED_PREFIXES}
    SELECT DISTINCT ?group WHERE {{
        ?dataset a dataid:Dataset .
        ?dataset dct:publisher <{publisher_uri}> .
        ?dataset dataid:group ?group .
    }}"""
    return fetch_query_result_by_key(sparql_endpoint, query, "group")


def load_artifacts(sparql_endpoint: str, group_id: str) -> list[str]:
    """Load artifacts for a given group ID"""
    query = f"""{USED_PREFIXES}
    SELECT DISTINCT ?artifact WHERE {{
        ?dataset dataid:group <{group_id}> .
        ?dataset dataid:artifact ?artifact .
    }}"""
    return fetch_query_result_by_key(sparql_endpoint, query, "artifact")


def load_versions(sparql_endpoint: str, artifact_id: str) -> list[str]:
    """Load versions for a given artifact ID"""
    query = f"""{USED_PREFIXES}
    SELECT DISTINCT ?version WHERE {{
        ?dataset dataid:artifact <{artifact_id}> .
        ?dataset dataid:version ?version .
    }}"""
    return fetch_query_result_by_key(sparql_endpoint, query, "version")


def load_files(sparql_endpoint: str, version_id: str) -> list[str]:
    """Load files for a given version ID"""
    query = f"""{USED_PREFIXES}
    SELECT DISTINCT ?file WHERE {{
        ?dataset dataid:version <{version_id}> .
        ?dataset dcat:distribution ?dist .
        ?dist dataid:file ?file.
    }}"""
    return fetch_query_result_by_key(sparql_endpoint, query, "file")


class DatabusFileAutocomplete(StringParameterType):
    """Class for autocompleting identifiers from an arbitrary databus"""

    def autocomplete(
        self,
        query_terms: list[str],
        depend_on_parameter_values: list[Any],  # noqa: ARG002
        context: PluginContext,  # noqa: ARG002
    ) -> list[Autocompletion]:
        """Return results that match provided query terms"""
        return self.__transform_uris_to_autocompletion(self.fetch_results_by_uri(query_terms[0]))

    @staticmethod
    def fetch_results_by_uri(query_str: str) -> list[str]:
        """Fetch results for completion for databus file identifiers"""
        query_no_http = query_str.replace("https://", "")
        parts = query_no_http.rstrip("/ ").rsplit("/", 5)

        endpoint = "https://" + parts[0] + "/sparql"

        normalized_query = query_str[0 : query_str.rfind("/")]

        result: list[str] = []
        try:
            if len(parts) == 1:
                # when it's only the databus -> fetch possible accounts
                result = load_accounts(endpoint)
            if len(parts) == 2:  # noqa: PLR2004
                # when it's the account return groups
                # needs #this appended for publisher id
                result = load_groups(endpoint, normalized_query + "#this")
            if len(parts) == 3:  # noqa: PLR2004
                # when it's a group -> return artifacts
                result = load_artifacts(endpoint, normalized_query)
            if len(parts) == 4:  # noqa: PLR2004
                # when it's an artifact -> return versions
                result = load_versions(endpoint, normalized_query)
            if len(parts) == 5:  # noqa: PLR2004
                # when it's a version -> return files
                result = load_files(endpoint, normalized_query)
        except RequestException:
            return [query_str]
        return result

    @staticmethod
    def __transform_uris_to_autocompletion(uri_list: list[str]) -> list[Autocompletion]:
        """Transform a list of URIs into a list of autocompletion"""
        return [Autocompletion(uri, uri) for uri in uri_list]


class WebDAVHandler:
    """Work with a WebDAV endpoint."""

    def __init__(self, databus_base: str, user: str, api_key: str):
        self.dav_base = databus_base + f"dav/{user}/"
        self.api_key = api_key

    def check_existence(self, path: str) -> bool:
        """Check if path is available"""
        try:
            resp = requests.head(url=f"{self.dav_base}{path}", timeout=4)
        except requests.RequestException:
            return False

        return bool(resp.status_code == http.HTTPStatus.METHOD_NOT_ALLOWED)

    def create_dir(self, path: str, session: requests.Session | None = None) -> requests.Response:
        """Create directory"""
        if session is None:
            session = requests.Session()

        req = requests.Request(
            method="MKCOL",
            url=f"{self.dav_base}{path}",
            headers={"X-API-KEY": f"{self.api_key}"},
        )
        return session.send(req.prepare())

    def create_dirs(self, path: str) -> list[requests.Response]:
        """Create directories"""
        dirs = path.split("/")
        responses = []
        current_path = ""
        for directory in dirs:
            current_path = current_path + directory + "/"
            if not self.check_existence(current_path):
                resp = self.create_dir(current_path)
                responses.append(resp)
                if resp.status_code not in [200, 201, 405]:
                    raise WebDAVError(resp)

        return responses

    def upload_file_with_context(  # noqa: PLR0913
        self,
        path: str,
        data: bytes,
        context: ExecutionContext,
        chunk_size: int,
        create_parent_dirs: bool = False,
    ) -> requests.Response:
        """Upload file + updating report (?)"""
        # pylint: disable=too-many-arguments

        context_data_generator = byte_iterator_context_update(
            data, context, desc="Uploading File", chunksize=chunk_size
        )

        if create_parent_dirs:
            dirpath = path.rsplit("/", 1)[0]
            responses = self.create_dirs(dirpath)
            # when list not empty (=> every dir existed) and last one was an error
            # -> raise exception
            if responses and responses[-1].status_code not in [200, 201, 405]:
                raise WebDAVError(responses[-1])

        return requests.put(
            url=f"{self.dav_base}{path}",
            headers={"X-API-KEY": f"{self.api_key}"},
            data=context_data_generator,
            stream=True,
            timeout=3000,
        )

    def upload_file(
        self, path: str, data: bytes, create_parent_dirs: bool = False
    ) -> requests.Response:
        """Upload data in bytes to a path, optionally creating parent dirs."""
        if create_parent_dirs:
            dirpath = path.rsplit("/", 1)[0]
            self.create_dirs(dirpath)

        return requests.put(
            url=f"{self.dav_base}{path}",
            headers={"X-API-KEY": f"{self.api_key}"},
            data=data,
            timeout=3000,
        )


def byte_iterator_context_update(
    data: bytes, context: ExecutionContext, chunksize: int, desc: str
) -> Iterator[bytes]:
    """Update Execution report"""
    for i, chunk in enumerate([data[i : i + chunksize] for i in range(0, len(data), chunksize)]):
        op_desc = f"{desc} {get_clock(i)}"
        context.report.update(
            ExecutionReport(
                entity_count=(i * chunksize) // 1000000,
                operation="wait",
                operation_desc=op_desc,
            )
        )
        yield chunk


def fetch_facets_options(databus_base: str, url_parameters: dict | None = None) -> dict:
    """Fetch facet options for a given document"""
    encoded_query_str = ""
    if url_parameters:
        encoded_query_str = urlencode(url_parameters)
    headers = {"Content-Type": "application/json"}
    request_uri = f"{databus_base}/app/utils/facets?{encoded_query_str}"
    json_resp = requests.get(request_uri, headers=headers, timeout=30).json()

    return {
        "version": json_resp["http://purl.org/dc/terms/hasVersion"]["values"],
        "format": json_resp["https://dataid.dbpedia.org/databus#formatExtension"]["values"],
    }


def fetch_databus_files(endpoint: str, artifact: str, version: str, file_format: str) -> list:
    """Fetch databus file name based of artifact, version and format on a given databus instance"""
    query = f"""{USED_PREFIXES}
    SELECT DISTINCT ?file ?version ?artifact ?license ?size ?format ?compression
    (GROUP_CONCAT(DISTINCT ?var; SEPARATOR=', ') AS ?variant) ?preview WHERE {{
        GRAPH ?g {{
            ?dataset databus:artifact <{artifact}> .
            {{ ?distribution <http://purl.org/dc/terms/hasVersion> '{version}' . }}
            {{ ?distribution databus:formatExtension '{file_format}' . }}
            ?dataset dcat:distribution ?distribution .
            ?distribution databus:file ?file .
            ?distribution databus:formatExtension ?format .
            ?distribution databus:compression ?compression .
            ?dataset dct:license ?license .
            ?dataset dct:hasVersion ?version .
            ?dataset databus:artifact ?artifact .
            OPTIONAL {{
                ?distribution ?p ?var.
                ?p rdfs:subPropertyOf databus:contentVariant .
            }}
            OPTIONAL {{ ?distribution dcat:byteSize ?size . }}
        }}
    }}
    GROUP BY ?file ?version ?artifact ?license ?size ?format ?compression ?preview"""

    endpoint = endpoint + "/sparql"
    sparql_service = SPARQLWrapper(endpoint)
    sparql_service.setQuery(query)
    sparql_service.setReturnFormat(JSON)

    query_results: dict = sparql_service.query().convert()  # type: ignore[assignment]
    bindings: list = query_results["results"]["bindings"]
    return bindings
