import hashlib
import json
from collections import OrderedDict
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.utils import (
    split_task_id,
    setup_cmempy_super_user_access,
)
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from databusclient import createDataset, deploy, create_distribution
from cmem.cmempy.dp.proxy.graph import get_streamed
from datetime import datetime
from cmem.cmempy.workspace.tasks import get_task
from typing import Tuple, List
from .databus_utils import WebDAVHandler, WebDAVException
from .cmem_utils import get_clock


@Plugin(
    label="Databus Deploy Plugin",
    description="Deploys a graph to the Databus",
    documentation="""
This CMEM task deploys a knowledge graph to the defined Databus Dataset.

The knowledge graph will be deployed as a turtle file to the Databus.
""",
    parameters=[
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
            default_value="",
        ),
        PluginParameter(
            name="license_uri",
            label="Dataset License URI",
            description="Define the URI of the license under which the Dataset should be published",
            # In the new version this should be a dropdown menu
            param_type=ChoiceParameterType(
                OrderedDict({'http://dalicc.net/licenselibrary/AcademicFreeLicense30': 'Academic Free License 3.0',
                             'http://dalicc.net/licenselibrary/AdaptivePublicLicense10': 'Adaptive Public License 1.0',
                             'http://dalicc.net/licenselibrary/ApplePublicSourceLicense20': 'Apple Public Source License 2.0',
                             'http://dalicc.net/licenselibrary/ArtisticLicense20': 'Artistic License 2.0',
                             'http://dalicc.net/licenselibrary/AttributionAssuranceLicense': 'Attribution Assurance License',
                             'http://dalicc.net/licenselibrary/BoostSoftwareLicense10': 'Boost Software License 1.0',
                             'http://dalicc.net/licenselibrary/CeaCnrsInriaLogicielLibreLicenseVersion21': 'Cea Cnrs Inria Logiciel Libre License, version 2.1',
                             'http://dalicc.net/licenselibrary/CommonDevelopmentAndDistributionLicense10': 'Common Development and Distribution License 1.0',
                             'http://dalicc.net/licenselibrary/CommonPublicAttributionLicenseVersion10': 'Common Public Attribution License Version 1.0',
                             'http://dalicc.net/licenselibrary/ComputerAssociatesTrustedOpenSourceLicense11': 'Computer Associates Trusted Open Source License 1.1'}
                            ))
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
            name="source_dataset",
            label="Source Dataset",
            description="Graph name to publish to the Databus",
            param_type=DatasetParameterType(dataset_type="eccencaDataPlatform"),
        ),
    ],
)
class DatabusDeployPlugin(WorkflowPlugin):
    def __init__(
            self,
            dataset_artifact_uri: str,
            version: str,
            license_uri: str,
            api_key: str,
            source_dataset: str,
            cvs: str,
    ) -> None:
        self.dataset_artifact_uri = dataset_artifact_uri
        databus_base, user, _, _ = self.__get_identifier_from_artifact()
        self.webdav_handler = WebDAVHandler(
            databus_base=databus_base + "/", user=user, api_key=api_key
        )
        self.version = version
        self.license_uri = license_uri
        self.api_key = api_key
        self.cvs = {}
        for cv_str in cvs.split(","):
            k, v = cv_str.split("=")
            self.cvs[k] = v
        self.fileformat = "ttl"
        self.source_dataset = source_dataset

    def __get_identifier_from_artifact(self) -> tuple[str, str, str, str]:
        databus_base, user, group, artifact = self.dataset_artifact_uri.rstrip(
            "/ "
        ).rsplit("/", 3)
        return str(databus_base), str(user), str(group), str(artifact)

    def __fetch_graph_metadata(
            self, context: ExecutionContext
    ) -> Tuple[str, str, str, str]:

        project_id = context.task.project_id()
        task_id = self.source_dataset
        metadata_dict = get_task(project=project_id, task=task_id)

        self.log.error(str(metadata_dict))

        uri = metadata_dict["data"]["parameters"]["graph"]["value"]
        title = metadata_dict["metadata"]["label"]
        description = metadata_dict["metadata"]["description"]
        abstract = _generate_abstract_from_description(description)

        return str(uri), str(title), str(abstract), str(description)

    def execute(
            self, inputs=(), context: ExecutionContext = ExecutionContext()
    ) -> None:

        # init summary and warnings
        summary: List[Tuple[str, str]] = []
        warnings: List[str] = []

        # handle version during execution, NOT during initialisation
        if self.version is None or self.version.strip(" ") == "":
            self.version = datetime.now().strftime("%Y.%m.%d")
            summary.append(("Version automatically set to", self.version))
            warnings.append(f"Version not hardcoded, automatically set to {self.version}")
        context.report.update(ExecutionReport(operation_desc=f"Started deploy of version {self.version}"))
        setup_cmempy_super_user_access()
        # deploy metadata to databus
        graph_uri, title, abstract, description = self.__fetch_graph_metadata(context)

        databus_base, user, group, artifact = self.__get_identifier_from_artifact()

        self.log.info(f"Info about graph: {title} ({graph_uri})")

        # generating some required strings
        cv_string = "_".join([f"{k}={v}" for k, v in self.cvs.items()])

        file_target_path = f"{group}/{artifact}/{self.version}/{artifact}_{cv_string}.{self.fileformat}"

        # fetch data
        graph_response = get_streamed(graph_uri, accept="text/turtle")
        data: bytearray = bytearray()
        cnt = 0
        chunk_size = 4096
        with graph_response as resp:
            for b in resp.iter_content(chunk_size=chunk_size):
                cnt += chunk_size
                data += bytearray(b)
                desc = f"Downloading File {get_clock(cnt)}"
                context.report.update(
                    ExecutionReport(entity_count=cnt, operation="wait", operation_desc=desc)
                )

        sha256sum = hashlib.sha256(bytes(data)).hexdigest()
        content_length = len(data)
        summary.append(("File sha256sum", str(sha256sum)))
        summary.append(("File size (bytes)", str(content_length)))

        context.report.update(ExecutionReport(operation_desc=f"Uploading file to {file_target_path}"))
        upload_resp = self.webdav_handler.upload_file_with_context(
            file_target_path, data=bytes(data), context=context, create_parent_dirs=True, chunksize=chunk_size
        )
        if upload_resp.status_code >= 400:
            raise WebDAVException(upload_resp)
        # self.__handle_webdav(file_target_path, graph_response.content)

        version_id = f"{databus_base}/{user}/{group}/{artifact}/{self.version}"
        file_url = f"{self.webdav_handler.dav_base}{file_target_path}"
        distrib = create_distribution(
            url=file_url, cvs=self.cvs, file_format=self.fileformat
        )
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
        context.report.update(ExecutionReport(operation_desc=f"Deployment to the Databus Successfull ✓"))


def _generate_abstract_from_description(description: str) -> str:
    first_point = description.find(".")
    if first_point == -1:
        first_sentence = description
    else:
        first_sentence = description[0: first_point + 1]

    return first_sentence
