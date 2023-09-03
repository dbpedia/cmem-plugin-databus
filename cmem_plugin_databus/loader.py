"""Plugin for loading one file from the databus and write it ino a dataset"""
from typing import Any, Optional

import requests
from cmem.cmempy.workspace.projects.resources import get_resources
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    PluginContext
)
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.types import StringParameterType, Autocompletion
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access

from cmem_plugin_databus.utils import (
    fetch_api_search_result, fetch_facets_options, fetch_databus_files,
)


class DatabusSearch(StringParameterType):
    """Databus Search Type"""

    autocompletion_depends_on_parameters: list[str] = ["databus_base_url"]

    # auto complete for values
    allow_only_autocompleted_values: bool = True
    # auto complete for labels
    autocomplete_value_with_labels: bool = True

    def autocomplete(
        self,
        query_terms: list[str],
        depend_on_parameter_values: list[Any],
        context: PluginContext,
    ) -> list[Autocompletion]:

        if not query_terms:
            label = "Search for Databus artifacts"
            return [Autocompletion(value="", label=f"{label}")]

        databus_base_url = depend_on_parameter_values[0]
        result = fetch_api_search_result(
            databus_base=databus_base_url,
            url_parameters={
                "query": " ".join(query_terms),
                "typeNameWeight": 0,
                "minRelevance": 20,
                "maxResults": 25,
                "typeName": " Artifact"
            }
        )
        return [
            Autocompletion(
                value=f"{_.resource}",
                label=f"{_.label}",
            )for _ in result
        ]


class FacetSearch(StringParameterType):
    """Facet Type"""

    autocompletion_depends_on_parameters: list[str] = [
        "databus_base_url",
        "databus_artifact"
    ]

    # auto complete for values
    allow_only_autocompleted_values: bool = True
    # auto complete for labels
    autocomplete_value_with_labels: bool = False
    #

    def __init__(self, facet_option: str):
        self.facet_option = facet_option

    def autocomplete(
        self,
        query_terms: list[str],
        depend_on_parameter_values: list[Any],
        context: PluginContext,
    ) -> list[Autocompletion]:

        databus_base_url = depend_on_parameter_values[0]
        databus_document = depend_on_parameter_values[1]
        result = fetch_facets_options(
            databus_base=databus_base_url,
            url_parameters={
                "type": "artifact",
                "uri": databus_document
            }
        )
        _format = result[self.facet_option]
        return [
            Autocompletion(
                value=f"{_}",
                label=f"{_}",
            )for _ in _format
        ]


class ResourceParameterType(StringParameterType):
    """Resource parameter type."""
    allow_only_autocompleted_values: bool = True

    autocomplete_value_with_labels: bool = True

    file_type: Optional[str] = None

    def __init__(self, file_type: Optional[str] = None):
        """Dataset parameter type."""
        self.file_type = file_type

    def autocomplete(
            self,
            query_terms: list[str],
            depend_on_parameter_values: list[Any],
            context: PluginContext,
    ) -> list[Autocompletion]:
        setup_cmempy_user_access(context.user)
        resources = get_resources(context.project_id)
        return [
            Autocompletion(
                value=f"{_['fullPath']}",
                label=f"{_['name']}",
            ) for _ in resources
        ]


class DatabusFile(StringParameterType):
    """Class for DatabusFile"""
    autocompletion_depends_on_parameters: list[str] = [
        "databus_base_url",
        "databus_artifact",
        "artifact_format",
        "artifact_version"
    ]

    # auto complete for values
    allow_only_autocompleted_values: bool = True
    # auto complete for labels
    autocomplete_value_with_labels: bool = False

    def autocomplete(
            self,
            query_terms: list[str],
            depend_on_parameter_values: list[Any],
            context: PluginContext,
    ) -> list[Autocompletion]:
        databus_base_url = depend_on_parameter_values[0]
        databus_document = depend_on_parameter_values[1]
        artifact_format = depend_on_parameter_values[2]
        artifact_version = depend_on_parameter_values[3]
        result = fetch_databus_files(
            endpoint=databus_base_url,
            artifact=databus_document,
            version=artifact_version,
            file_format=artifact_format
        )
        return [
            Autocompletion(
                value=f"{_['file']['value']}",
                label=f"{_['version']['value']}:"
                      f"{_['variant']['value']}:"
                      f"{_['format']['value']}:"
                      f"{_['compression']['value']}",
            ) for _ in result
        ]


class ResponseStream:
    """A context manager for streaming the content of an HTTP response in chunks.

    This class allows you to stream the content of an HTTP response in manageable chunks
    without loading the entire response into memory at once. It provides an iterable
    interface to read the response content piece by piece."""

    def __enter__(self):
        return self._read()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, response, chunk_size=1048576):
        self.response = response
        self.chunk_size = chunk_size

    def _read(self):
        for _ in self.response.iter_content(chunk_size=self.chunk_size):
            yield _


@Plugin(
    label="Simple Databus Loading Plugin",
    description="Loads a specfic file from the Databus to a local directory",
    documentation="""
This CMEM task loads a file from the defined Databus to a RDF dataset.
""",
    parameters=[
        PluginParameter(
            name="databus_base_url",
            label="Databus Base URL",
            description="The URL of the databus server",
        ),
        PluginParameter(
            name="databus_artifact",
            label="Artifact",
            description="The name of databus artifact",
            param_type=DatabusSearch(),
            default_value=""
        ),
        PluginParameter(
            name="artifact_format",
            label="Format",
            description="The format of databus artifact",
            param_type=FacetSearch(facet_option="format"),
            default_value=""
        ),
        PluginParameter(
            name="artifact_version",
            label="Version",
            description="The version of databus artifact",
            param_type=FacetSearch(facet_option="version"),
            default_value=""
        ),
        PluginParameter(
            name="databus_file_id",
            label="Databus File ID",
            description="The Databus file id of the file to download",
            param_type=DatabusFile()
        ),
        PluginParameter(
            name="target_file",
            label="File",
            description="File name to save the response from the Databus.",
            param_type=ResourceParameterType(),
        ),
        PluginParameter(
            name="chunk_size",
            label="Chunk Size",
            description="Chunksize during up/downloading the graph",
            default_value=1048576,
            advanced=True,
        ),
    ],
)
class SimpleDatabusLoadingPlugin(WorkflowPlugin):
    """Implementation of loading one file from the Databus into a given dataset"""

    # pylint: disable=too-many-arguments
    def __init__(
            self,
            databus_base_url: str,
            databus_artifact: str,
            artifact_format: str,
            artifact_version: str,
            databus_file_id: str,
            target_file: str,
            chunk_size: int
    ) -> None:
        self.databus_url = databus_base_url
        self.databus_file_id = databus_file_id
        self.target_file = target_file
        self.chunk_size = chunk_size
        # to get rid of unused-argument
        _ = databus_artifact
        _ = artifact_format
        _ = artifact_version

    def execute(
        self, inputs=(), context: ExecutionContext = ExecutionContext()
    ) -> None:
        setup_cmempy_user_access(context.user)
        self.log.info(f"Loading file from {self.databus_file_id}")

        databus_file_resp = requests.get(
            self.databus_file_id,
            allow_redirects=True,
            stream=True,
            timeout=3000,
        )

        if databus_file_resp.status_code > 400:
            context.report.update(
                ExecutionReport(
                    operation_desc="Download Failed ❌", error=databus_file_resp.text
                )
            )
            return

        upload_response = create_resource(
            project_name=context.task.project_id(),
            resource_name=self.target_file,
            file_resource=ResponseStream(databus_file_resp),
            replace=True
        )
        if upload_response.status_code < 400:
            context.report.update(ExecutionReport(operation_desc="Upload Successful ✓"))
        else:
            context.report.update(
                ExecutionReport(
                    operation_desc="Upload Failed ❌",
                    error=upload_response.text
                )
            )
