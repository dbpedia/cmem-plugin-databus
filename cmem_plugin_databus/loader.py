"""Plugin for loading one file from the databus and write it ino a dataset"""

import io
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.utils import (
    setup_cmempy_super_user_access,
    split_task_id,
)
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
import requests
from .utils import (
    DatabusFileAutocomplete,
    byte_iterator_context_update,
    post_streamed_bytes,
    get_clock,
)
from cmem.cmempy.workspace.tasks import get_task


@Plugin(
    label="Simple Databus Loading Plugin",
    description="Loads a specfic file from the Databus to a local directory",
    documentation="""
This CMEM task loads a file from the defined Databus to a RDF dataset.
""",
    parameters=[
        PluginParameter(
            name="target_graph",
            label="Target Graph",
            description="Graph name to save the response from the Databus.",
            param_type=DatasetParameterType(dataset_type="eccencaDataPlatform"),
        ),
        PluginParameter(
            name="databus_file_id",
            label="Databus File ID",
            description="The Databus file id of the file to download",
            param_type=DatabusFileAutocomplete(),
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
        self, databus_file_id: str, target_graph: str, chunk_size: int
    ) -> None:
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
            self.databus_file_id, allow_redirects=True, stream=True
        )

        if databus_file_resp.status_code > 400:
            context.report.update(
                ExecutionReport(
                    operation_desc="Download Failed ❌", error=databus_file_resp.text
                )
            )
            return

        with databus_file_resp as resp:
            for i, b in enumerate(resp.iter_content(chunk_size=self.chunk_size)):
                data += bytearray(b)
                desc = f"Downloading File {get_clock(i)}"
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
