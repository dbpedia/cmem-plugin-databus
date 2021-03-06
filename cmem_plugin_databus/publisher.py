import hashlib
import json
from collections import OrderedDict
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.utils import split_task_id, setup_cmempy_super_user_access
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from databusclient import createDataset, deploy, create_distribution
from webdav3.client import Client as WebDAVClient
from cmem.cmempy.dp.proxy.graph import get_streamed
from datetime import datetime
from cmem.cmempy.workspace.tasks import get_task
from webdav3.urn import Urn
from typing import Tuple


@Plugin(
    label="Databus Deploy Plugin",
    description="Deploys a graph to the Databus",
    documentation="""
This CMEM task deploys a knowledge graph to the defined Databus Dataset.

The knowledge graph will be deployed as a turtle file to the Databus.
""",
    parameters=[
        PluginParameter(
            name="webdav_hostname",
            label="WebDAV Hostname URI",
            description="The URI to deploy the data to via WebDAV. NOTE: The credentials for WebDAV must be correct "
                        "for this URI!",
        ),
        PluginParameter(
            name="webdav_credentials",
            label="WebDAV Target Credentials",
            description="The credentials for the WebDAV. Submitted in the form of login:password",
        ),
        PluginParameter(
            name="dataset_artifact_uri",
            label="Dataset Artifact URI",
            description="The Databus Dataset Artifact for this specific dataset. It conforms to following conventions: "
                        "https://{DATABUS_BASE_URI}/{PUBLISHER}/{GROUP}/{ARTIFACT}/",
        ),
        PluginParameter(
            name="version",
            label="Dataset Version",
            description="The version of the Dataset. If omitted, it is automatically set to YYYY.MM.DD. NOTE: This "
                        "can overwrite already published Datasets on the Databus!",
        ),
        PluginParameter(
            name="license_uri",
            label="Dataset License URI",
            description="Define the URI of the license under which the Dataset should be published",
            # In the new version htis should be a dropdown menu
            # param_type=ChoiceParameterType(
            #     OrderedDict({'Academic Free License 3.0': 'http://dalicc.net/licenselibrary/AcademicFreeLicense30',
            #                  'Adaptive Public License 1.0': 'http://dalicc.net/licenselibrary/AdaptivePublicLicense10',
            #                  'Apple Public Source License 2.0': 'http://dalicc.net/licenselibrary/ApplePublicSourceLicense20',
            #                  'Artistic License 2.0': 'http://dalicc.net/licenselibrary/ArtisticLicense20',
            #                  'Attribution Assurance License': 'http://dalicc.net/licenselibrary/AttributionAssuranceLicense',
            #                  'Boost Software License 1.0': 'http://dalicc.net/licenselibrary/BoostSoftwareLicense10',
            #                  'Cea Cnrs Inria Logiciel Libre License, version 2.1': 'http://dalicc.net/licenselibrary/CeaCnrsInriaLogicielLibreLicenseVersion21',
            #                  'Common Development and Distribution License 1.0': 'http://dalicc.net/licenselibrary/CommonDevelopmentAndDistributionLicense10',
            #                  'Common Public Attribution License Version 1.0': 'http://dalicc.net/licenselibrary/CommonPublicAttributionLicenseVersion10',
            #                  'Computer Associates Trusted Open Source License 1.1': 'http://dalicc.net/licenselibrary/ComputerAssociatesTrustedOpenSourceLicense11'}
            #                 ))
        ),
        PluginParameter(
            name="api_key",
            label="API KEY",
            description="An API Key of your Databus Account. Can be found/created at $DATABUS_BASE/$ACCOUNT#settings",
        ),
        PluginParameter(
            name="cvs",
            label="Content Variants",
            description="Key-Value-Pairs identifying a File uniquely. Example: key1=val1,key2=val2",
        ),
        PluginParameter(
            name="source_graph",
            label="Source Graph",
            description="Graph name to publish to the Databus",
            param_type=DatasetParameterType(dataset_type="eccencaDataPlatform"),
        )
    ]
)
class DatabusDeployPlugin(WorkflowPlugin):

    def __init__(
            self,
            webdav_hostname: str,
            webdav_credentials: str,
            dataset_artifact_uri: str,
            version: str,
            license_uri: str,
            api_key: str,
            source_graph: str,
            cvs: str,
    ) -> None:
        self.webdav_hostname = webdav_hostname
        webdav_login, webdav_pw = webdav_credentials.split(":")
        self.webdav_client = WebDAVClient({
            'webdav_hostname': webdav_hostname,
            'webdav_login': webdav_login,
            'webdav_password': webdav_pw
        })
        self.dataset_artifact_uri = dataset_artifact_uri
        self.version = version
        self.license_uri = license_uri
        self.api_key = api_key
        self.cvs = {}
        for cv_str in cvs.split(","):
            k, v = cv_str.split("=")
            self.cvs[k] = v
        self.fileformat = "ttl"
        self.source_graph = source_graph

    def __handle_webdav_dirs(self, user: str, group: str, artifact: str, version: str):
        required_dirs = [
            f"{user}",
            f"{user}/{group}",
            f"{user}/{group}/{artifact}",
            f"{user}/{group}/{artifact}/{version}"
        ]

        for req_dir in required_dirs:
            if not self.webdav_client.check(req_dir):
                self.webdav_client.mkdir(req_dir)

    def __webdav_upload_bytes(self, target_path: str, data: bytes) -> None:
        urn = Urn(target_path)
        resp = self.webdav_client.execute_request(action='upload', path=urn.quote(), data=data)
        self.log.info(f"Status file upload: {str(resp.status_code)}")

    def __get_identifier_from_artifact(self) -> tuple[str, str, str, str]:
        databus_base, user, group, artifact = self.dataset_artifact_uri.rstrip("/ ").rsplit("/", 3)
        return str(databus_base), str(user), str(group), str(artifact)

    def __handle_webdav(self, target_path: str, graph_data: bytes) -> None:

        _, user, group, artifact = self.__get_identifier_from_artifact()

        self.__handle_webdav_dirs(user, group, artifact, self.version)

        self.__webdav_upload_bytes(target_path, graph_data)

    def __fetch_graph_metadata(self) -> Tuple[str, str, str, str]:

        project_id, task_id = split_task_id(self.source_graph)
        metadata_dict = get_task(project=project_id, task=task_id)

        self.log.error(str(metadata_dict))

        uri = metadata_dict["data"]["parameters"]["graph"]["value"]
        title = metadata_dict["metadata"]["label"]
        description = metadata_dict["metadata"]["description"]
        abstract = _generate_abstract_from_description(description)

        return str(uri), str(title), str(abstract), str(description)

    def execute(self, inputs=(), **kwargs) -> None:
        # handle version during execution, NOT during initialisation
        if self.version is None or self.version.strip(" ") == "":
            self.version = datetime.now().strftime("%Y.%m.%d")

        setup_cmempy_super_user_access()
        # deploy metadata to databus
        graph_uri, title, abstract, description = self.__fetch_graph_metadata()

        databus_base, user, group, artifact = self.__get_identifier_from_artifact()

        self.log.info(f"Info about graph: {title}")

        # generating some required strings
        cv_string = "_".join([f"{k}={v}" for k, v in self.cvs.items()])

        file_target_path = f"{user}/{group}/{artifact}/{self.version}/{artifact}_{cv_string}.{self.fileformat}"

        # fetch data
        graph_response = get_streamed(graph_uri, accept="text/turtle")

        # sha256sum = hashlib.sha256(bytes(graph_response.content)).hexdigest()
        #
        # content_length = len(graph_response.content)

        self.__handle_webdav(file_target_path, graph_response.content)

        version_id = f"{databus_base}/{user}/{group}/{artifact}/{self.version}"
        file_url = f"{self.webdav_hostname}/{file_target_path}"
        distrib = create_distribution(url=file_url, cvs=self.cvs, file_format=self.fileformat)
        self.log.info(f"Distrib String: {distrib}")
        dataset = createDataset(
            versionId=version_id,
            title=title,
            abstract=abstract,
            description=description,
            license=self.license_uri,
            distributions=[distrib],
        )
        self.log.info(f"Submitted Dataset to Databus: {json.dumps(dataset)}")
        deploy(dataset, self.api_key)


def _generate_abstract_from_description(description: str) -> str:
    first_point = description.find(".")
    if first_point == -1:
        first_sentence = description
    else:
        first_sentence = description[0:first_point + 1]

    return first_sentence
