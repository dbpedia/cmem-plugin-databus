"""Plugin for loading one file from the databus and write it ino a dataset"""

import io
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.utils import (
    write_to_dataset,
    setup_cmempy_super_user_access,
    split_task_id,
)
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
import requests
from .databus_utils import DatabusFileAutocomplete
from .cmem_utils import post_streamed_bytesio
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
    ],
)
class SimpleDatabusLoadingPlugin(WorkflowPlugin):
    """Implementation of loading one file from the Databus into a given dataset"""

    def __init__(self, databus_file_id: str, target_graph: str) -> None:
        self.databus_file_id = databus_file_id
        self.target_graph = target_graph

    def execute(self, inputs=()) -> None:

        setup_cmempy_super_user_access()
        self.log.info(f"Loading file from {self.databus_file_id}")
        resp = requests.get(self.databus_file_id, allow_redirects=True)
        if resp.status_code < 400:
            # write_to_dataset(self.target_dataset, io.BytesIO(resp.content))
            project_id, task_id = split_task_id(self.target_graph)
            graph_uri = get_task(project=project_id, task=task_id)["data"]["parameters"]["graph"]["value"]
            post_streamed_bytesio(str(graph_uri), io.BytesIO(resp.content), replace=True)
        else:
            raise FileNotFoundError
