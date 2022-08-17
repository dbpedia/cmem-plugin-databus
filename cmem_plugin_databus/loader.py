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
from .utils import DatabusFileAutocomplete, byte_iterator_context_update, post_streamed_bytes, get_clock
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
            default_value=4096,
            advanced=True,
        )
    ],
)
class SimpleDatabusLoadingPlugin(WorkflowPlugin):
    """Implementation of loading one file from the Databus into a given dataset"""

    def __init__(self, databus_file_id: str, target_graph: str, chunk_size: int) -> None:
        self.databus_file_id = databus_file_id
        self.target_graph = target_graph
        self.chunk_size = chunk_size

    def execute(self, inputs=(), context: ExecutionContext = ExecutionContext()) -> None:

        setup_cmempy_super_user_access()
        self.log.info(f"Loading file from {self.databus_file_id}")

        data: bytearray = bytearray()
        with requests.get(self.databus_file_id, allow_redirects=True, stream=True) as resp:
            for i, b in enumerate(resp.iter_content(chunk_size=self.chunk_size)):
                data += bytearray(b)
                desc = f"Downloading File {get_clock(i)}"
                context.report.update(
                    ExecutionReport(entity_count=i * self.chunk_size, operation="wait", operation_desc=desc)
                )
        if resp.status_code < 400:
            # write_to_dataset(self.target_dataset, io.BytesIO(resp.content))
            project_id, task_id = split_task_id(self.target_graph)
            graph_uri = get_task(project=project_id, task=task_id)["data"][
                "parameters"
            ]["graph"]["value"]
            post_streamed_bytes(
                str(graph_uri),
                byte_iterator_context_update(bytes(data), context, self.chunk_size, "Uploading File"),
                replace=True
            )
        else:
            raise FileNotFoundError(self.databus_file_id)
