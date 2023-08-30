"""Plugin for loading one file from the databus and write it ino a dataset"""
from typing import Any

import requests
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport, \
    PluginContext
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.types import StringParameterType, Autocompletion
from cmem_plugin_base.dataintegration.utils import setup_cmempy_super_user_access

from cmem_plugin_databus.cmem_wrappers import post_streamed_bytes
from cmem_plugin_databus.utils import (
    byte_iterator_context_update,
    get_clock, fetch_api_search_result, fetch_facets_options, fetch_databus_files,
)


class DatabusSearch(StringParameterType):
    """Databus Search Type"""

    autocompletion_depends_on_parameters: list[str] = ["databus_base_url"]

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
        "databus_document"
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


class DatabusFile(StringParameterType):
    autocompletion_depends_on_parameters: list[str] = [
        "databus_base_url",
        "databus_document",
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
                label=f"{_['version']['value']}:{_['variant']['value']}:{_['format']['value']}:{_['compression']['value']}",
            ) for _ in result
        ]


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
            description="The URL of the Databus server",
        ),
        PluginParameter(
            name="databus_document",
            label="Databus Document",
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
            name="target_graph",
            label="Target Graph",
            description="Graph name to save the response from the Databus.",
            param_type=DatasetParameterType(dataset_type="eccencaDataPlatform"),
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

    def __init__(
            self,
            databus_base_url: str,
            databus_document: str,
            artifact_format: str,
            artifact_version: str,
            databus_file_id: str,
            target_graph: str,
            chunk_size: int
    ) -> None:
        self.databus_url = databus_base_url
        self.databus_file_id = databus_file_id
        self.target_graph = target_graph
        self.chunk_size = chunk_size

    def __get_graph_uri(self, context: ExecutionContext):
        task_info = get_task(project=context.task.project_id(), task=self.target_graph)
        return task_info["data"]["parameters"]["graph"]["value"]

    def execute(
        self, inputs=(), context: ExecutionContext = ExecutionContext()
    ) -> None:
        setup_cmempy_super_user_access()
        self.log.info(f"Loading file from {self.databus_file_id}")

        data: bytearray = bytearray()
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

        with databus_file_resp as resp:
            for _, chunk in enumerate(resp.iter_content(chunk_size=self.chunk_size)):
                data += bytearray(chunk)
                desc = f"Downloading File {get_clock(_)}"
                context.report.update(
                    ExecutionReport(
                        entity_count=len(data) // 1000000,
                        operation="load",
                        operation_desc=desc,
                    )
                )
        graph_uri = self.__get_graph_uri(context)
        post_resp = post_streamed_bytes(
            str(graph_uri),
            byte_iterator_context_update(
                bytes(data), context, self.chunk_size, "Uploading File"
            ),
            replace=True,
        )
        if post_resp.status_code < 400:
            context.report.update(ExecutionReport(operation_desc="Upload Successful ✓"))
        else:
            context.report.update(
                ExecutionReport(operation_desc="Upload Failed ❌", error=post_resp.text)
            )
