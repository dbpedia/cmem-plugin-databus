"""Deploys a graph to the Databus"""
import re
import hashlib
import json
from collections import OrderedDict
from datetime import datetime
from typing import List, Tuple

from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import setup_cmempy_super_user_access
from databusclient import create_distribution, createDataset, deploy

from cmem_plugin_databus.utils import (
    WebDAVException,
    WebDAVHandler,
    get_clock,
    MissingMetadataException,
)
from cmem_plugin_databus.cmem_wrappers import get_streamed

NS = "http://dalicc.net/licenselibrary/"

LICENSES = OrderedDict(
    {
        f"{NS}AcademicFreeLicense30": "Academic Free License 3.0",
        f"{NS}AdaptivePublicLicense10": "Adaptive Public License 1.0",
        f"{NS}ApplePublicSourceLicense20": "Apple Public Source License 2.0",
        f"{NS}ArtisticLicense20": "Artistic License 2.0",
        f"{NS}AttributionAssuranceLicense": "Attribution Assurance License",
        f"{NS}BoostSoftwareLicense10": "Boost Software License 1.0",
        f"{NS}CeaCnrsInriaLogicielLibreLicenseVersion21": (
            "Cea Cnrs Inria Logiciel Libre License, version 2.1"
        ),
        f"{NS}CommonDevelopmentAndDistributionLicense10": (
            "Common Development and Distribution License 1.0"
        ),
        f"{NS}CommonPublicAttributionLicenseVersion10": (
            "Common Public Attribution License Version 1.0"
        ),
        f"{NS}ComputerAssociatesTrustedOpenSourceLicense11": (
            "Computer Associates Trusted Open Source License 1.1"
        ),
    }
)


def validate_dataset_artifact_uri(uri: str):
    """validate dataset artifact uri"""
    pattern = r"^https?://[^/]+/[^/]+/[^/]+/[^/]+/$"

    if re.match(pattern, uri):
        return True
    return False


@Plugin(
    label="Publish to a DBpedia Databus",
    description="Deploys a graph to a Databus",
    documentation="""
This CMEM task deploys a knowledge graph to the defined Databus Dataset.

The knowledge graph will be deployed as a turtle file to the Databus.
""",
    parameters=[
        PluginParameter(
            name="dataset_artifact_uri",
            label="Dataset Artifact URI",
            description="The Databus Dataset Artifact for this specific dataset."
            " It conforms to following conventions: "
            "https://{DATABUS_BASE_URI}/{PUBLISHER}/{GROUP}/{ARTIFACT}/",
        ),
        PluginParameter(
            name="version",
            label="Dataset Version",
            description="The version of the Dataset. If omitted, it is automatically"
            " set to YYYY.MM.DD. NOTE: This can overwrite already"
            " published Datasets on the Databus!",
            default_value="",
        ),
        PluginParameter(
            name="license_uri",
            label="Dataset License URI",
            description="Define the URI of the license under which the "
            "Dataset should be published",
            # In the new version this should be a dropdown menu
            param_type=ChoiceParameterType(LICENSES),
        ),
        PluginParameter(
            name="api_key",
            label="API KEY",
            description="An API Key of your Databus Account."
            " Can be found/created at $DATABUS_BASE/$ACCOUNT#settings",
        ),
        PluginParameter(
            name="cvs",
            label="Content Variants",
            description="Key-Value-Pairs identifying a File uniquely."
            " Example: key1=val1,key2=val2",
        ),
        PluginParameter(
            name="source_dataset",
            label="Source Dataset",
            description="Graph name to publish to the Databus",
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
class DatabusDeployPlugin(WorkflowPlugin):
    """Deploys a graph to the Databus"""

    # pylint: disable=too-many-instance-attributes

    def __init__(
        self,
        dataset_artifact_uri: str,
        version: str,
        license_uri: str,
        api_key: str,
        source_dataset: str,
        cvs: str,
        chunk_size: int,
    ) -> None:
        # pylint: disable=too-many-arguments
        if validate_dataset_artifact_uri(dataset_artifact_uri):
            raise ValueError("The specified dataset artifact uri is not valid")
        self.dataset_artifact_uri = dataset_artifact_uri
        databus_base, user, _, _ = self.__get_identifier_from_artifact()
        self.webdav_handler = WebDAVHandler(
            databus_base=databus_base + "/", user=user, api_key=api_key
        )
        self.version = version
        self.license_uri = license_uri
        self.api_key = api_key
        self.cvs = {}
        for content_variant in cvs.split(","):
            key, value = content_variant.split("=")
            self.cvs[key] = value
        self.fileformat = "ttl"
        self.source_dataset = source_dataset
        self.chunk_size = chunk_size

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

        self.log.info("Fetched " + str(metadata_dict))

        try:
            uri: str = metadata_dict["data"]["parameters"]["graph"]["value"]
            title: str = metadata_dict["metadata"]["label"]
            description: str = metadata_dict["metadata"]["description"]
            abstract: str = _generate_abstract_from_description(description)
        except KeyError as key_err:
            raise MissingMetadataException(
                f"CMEM task {task_id}", key_err.args[0]
            ) from key_err

        for name, text in {"label": title, "description": description}.items():
            if text.strip() == "":
                raise MissingMetadataException(f"CMEM task {task_id}", name)

        return str(uri), str(title), str(abstract), str(description)

    def execute(
        self, inputs=(), context: ExecutionContext = ExecutionContext()
    ) -> None:
        # pylint: disable=too-many-locals

        # init summary and warnings
        summary: List[Tuple[str, str]] = []
        warnings: List[str] = []

        # handle version during execution, NOT during initialisation
        if self.version is None or self.version.strip(" ") == "":
            self.version = datetime.now().strftime("%Y.%m.%d")
            summary.append(("Version automatically set to", self.version))
            warnings.append(
                f"Version not hardcoded, automatically set to {self.version}"
            )
        context.report.update(
            ExecutionReport(operation_desc=f"Started deploy of version {self.version}")
        )
        setup_cmempy_super_user_access()
        # deploy metadata to databus
        try:
            graph_uri, title, abstract, description = self.__fetch_graph_metadata(
                context
            )
        except MissingMetadataException as mm_exception:
            context.report.update(ExecutionReport(error=str(mm_exception)))
            return

        databus_base, user, group, artifact = self.__get_identifier_from_artifact()

        self.log.info(f"Info about graph: {title} ({graph_uri})")

        # generating some required strings
        cv_string = "_".join([f"{k}={v}" for k, v in self.cvs.items()])

        file_target_path = (
            f"{group}/{artifact}/{self.version}/"
            f"{artifact}_{cv_string}.{self.fileformat}"
        )

        # fetch data
        data: bytearray = bytearray()
        with get_streamed(graph_uri, accept="text/turtle") as resp:
            for _, chunk in enumerate(resp.iter_content(chunk_size=self.chunk_size)):
                data += bytearray(chunk)
                desc = f"Get graph stream {get_clock(_)}"
                context.report.update(
                    ExecutionReport(
                        entity_count=len(data) // 1000000,
                        operation="wait",
                        operation_desc=desc,
                    )
                )

        sha256sum = hashlib.sha256(bytes(data)).hexdigest()
        content_length = len(data)
        summary.append(("File sha256sum", str(sha256sum)))
        summary.append(("File size (bytes)", str(content_length)))

        context.report.update(
            ExecutionReport(operation_desc=f"Uploading file to {file_target_path}")
        )
        upload_resp = self.webdav_handler.upload_file_with_context(
            path=file_target_path,
            data=bytes(data),
            context=context,
            create_parent_dirs=True,
            chunk_size=self.chunk_size,
        )
        if upload_resp.status_code >= 400:
            raise WebDAVException(upload_resp)

        context.report.update(
            ExecutionReport(operation_desc="WebDAV Upload Successful ✓")
        )

        version_id = f"{databus_base}/{user}/{group}/{artifact}/{self.version}"
        file_url = f"{self.webdav_handler.dav_base}{file_target_path}"
        distrib = create_distribution(
            url=file_url,
            cvs=self.cvs,
            file_format=self.fileformat,
            sha256_length_tuple=(sha256sum, content_length),
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
        context.report.update(ExecutionReport(operation_desc="Deployment Successful ✓"))


def _generate_abstract_from_description(description: str) -> str:
    first_point = description.find(".")
    if first_point == -1:
        first_sentence = description
    else:
        first_sentence = description[0 : first_point + 1]

    return first_sentence
