import io

from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.utils import write_to_dataset
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
import requests
from .databus_utils import DatabusFileAutocomplete

@Plugin(
    label="Simple Databus Loading Plugin",
    description="Loads a specfic file from the Databus to a local directory",
    documentation="""
This CMEM task loads a file from the defined Databus to a RDF dataset.
""",
    parameters=[
        PluginParameter(
            name="target_dataset",
            label="Target Dataset",
            description="Dateset name to save the response from the Databus",
            param_type=DatasetParameterType()
        ),
        PluginParameter(
            name="databus_file_id",
            label="Databus File ID",
            description="The Databus file id of the file to download",
            param_type=DatabusFileAutocomplete()
        ),
    ]
)
class SimpleDatabusLoadingPlugin(WorkflowPlugin):

    def __init__(
            self,
            target_dataset: str,
            databus_file_id: str
    ) -> None:
        self.databus_file_id = databus_file_id
        self.target_dataset = target_dataset

    def execute(self, inputs=()) -> None:

        resp = requests.get(self.databus_file_id, allow_redirects=True)
        if resp.status_code < 400:
            write_to_dataset(self.target_dataset, io.BytesIO(resp.content))
        else:
            raise FileNotFoundError
