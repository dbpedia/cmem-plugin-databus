"""Tests for the loader plugin"""
import io
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import pytest
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource, resource_exist
from cmem_plugin_base.dataintegration.context import PluginContext
from cmem_plugin_base.dataintegration.types import ParameterType

from cmem_plugin_databus.loader import (
    DatabusFile,
    DatabusSearch,
    FacetSearch,
    ResourceParameterType,
    SimpleDatabusLoadingPlugin,
)
from tests.utils import TestExecutionContext, TestPluginContext, needs_cmem


@dataclass
class FixtureDataLoading:
    """FixtureData Loader Testing"""

    project_name: str = "databus_sample_project"
    resource_name: str = "sample_test.txt"
    databus_base_url: str = "https://databus.dbpedia.org"
    databus_document: str = f"{databus_base_url}/cmempydeveloper/CorporateMemory/Documentation"
    document_version: str = "23.01"
    document_format: str = "md"
    databus_file: str = f"{databus_document}/{document_version}/Documentation.md"


def get_autocomplete_values(
    parameter: ParameterType,
    query_terms: list[str],
    depend_on_parameter_values: list[Any],
    context: PluginContext,
) -> list:
    """Get autocomplete values"""
    if depend_on_parameter_values is None:
        depend_on_parameter_values = []
    return [
        x.value
        for x in parameter.autocomplete(
            query_terms=query_terms,
            depend_on_parameter_values=depend_on_parameter_values,
            context=context,
        )
    ]


@pytest.fixture(name="project")
def project() -> Generator[FixtureDataLoading, Any, None]:
    """Provide the DI build project incl. assets."""
    _ = FixtureDataLoading()
    make_new_project(_.project_name)
    create_resource(
        project_name=_.project_name,
        resource_name=_.resource_name,
        file_resource=io.StringIO("SAMPLE CONTENT"),
        replace=True,
    )
    yield _
    delete_project(_.project_name)


@needs_cmem
def test_databus_load(project: FixtureDataLoading) -> None:
    """Test databus load"""
    _ = project
    resource_name = "upload_readme.md"
    databus_load = SimpleDatabusLoadingPlugin(
        databus_base_url=_.databus_base_url,
        databus_artifact="",
        artifact_format="",
        artifact_version="",
        databus_file_id="https://databus.dbpedia.org/cmempydeveloper/CorporateMemory"
        "/Documentation/23.02/Documentation.md",
        target_file=resource_name,
    )
    databus_load.execute(inputs=(), context=TestExecutionContext(project_id=_.project_name))
    assert resource_exist(project_name=_.project_name, resource_name=resource_name)


@needs_cmem
def test_databus_search_auto_complete(project: FixtureDataLoading) -> None:
    """Test databus search autocompletion"""
    _ = project
    parameter = DatabusSearch()
    assert "" in get_autocomplete_values(
        parameter, [], depend_on_parameter_values=[], context=TestPluginContext()
    )

    assert (
        len(
            get_autocomplete_values(
                parameter,
                ["NOTFOUND"],
                depend_on_parameter_values=[_.databus_base_url],
                context=TestPluginContext(),
            )
        )
        == 0
    )


@needs_cmem
def test_resource_parameter_type_completion(project: FixtureDataLoading) -> None:
    """Test resource parameter type completion"""
    _ = project
    project_name = _.project_name
    resource_name = _.resource_name
    parameter = ResourceParameterType()
    context = TestPluginContext(project_name)
    assert resource_name in get_autocomplete_values(
        parameter, [], depend_on_parameter_values=[], context=context
    )
    new_resource_name = "lkshfkdsjfhsd"
    assert (
        len(
            get_autocomplete_values(
                parameter, [new_resource_name], depend_on_parameter_values=[], context=context
            )
        )
        == 1
    )
    assert new_resource_name in get_autocomplete_values(
        parameter, [new_resource_name], depend_on_parameter_values=[], context=context
    )


@needs_cmem
def test_facet_search_auto_complete(project: FixtureDataLoading) -> None:
    """Test facet search autocompletion"""
    _ = project
    parameter = FacetSearch(facet_option="format")
    assert _.document_format in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[_.databus_base_url, _.databus_document],
        context=TestPluginContext(),
    )

    assert (
        len(
            get_autocomplete_values(
                parameter,
                ["NOTFOUND"],
                depend_on_parameter_values=[_.databus_base_url, _.databus_document],
                context=TestPluginContext(),
            )
        )
        == 0
    )

    parameter = FacetSearch(facet_option="version")
    assert _.document_version in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[_.databus_base_url, _.databus_document],
        context=TestPluginContext(),
    )
    assert _.document_version not in get_autocomplete_values(
        parameter,
        ["23.02"],
        depend_on_parameter_values=[_.databus_base_url, _.databus_document],
        context=TestPluginContext(),
    )
    assert (
        len(
            get_autocomplete_values(
                parameter,
                [],
                depend_on_parameter_values=[_.databus_base_url, _.databus_document],
                context=TestPluginContext(),
            )
        )
        == 2  # noqa: PLR2004
    )
    assert (
        len(
            get_autocomplete_values(
                parameter,
                ["NOTFOUND"],
                depend_on_parameter_values=[_.databus_base_url, _.databus_document],
                context=TestPluginContext(),
            )
        )
        == 0
    )


@needs_cmem
def test_databus_file_auto_complete(project: FixtureDataLoading) -> None:
    """Test databus file completion"""
    _ = project
    parameter = DatabusFile()
    assert _.databus_file in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[
            _.databus_base_url,
            _.databus_document,
            _.document_format,
            _.document_version,
        ],
        context=TestPluginContext(),
    )
    assert (
        len(
            get_autocomplete_values(
                parameter,
                [],
                depend_on_parameter_values=[
                    _.databus_base_url,
                    _.databus_document,
                    _.document_format,
                    _.document_version,
                ],
                context=TestPluginContext(),
            )
        )
        == 1
    )
    assert (
        len(
            get_autocomplete_values(
                parameter,
                ["ADSLASD"],
                depend_on_parameter_values=[
                    _.databus_base_url,
                    _.databus_document,
                    _.document_format,
                    _.document_version,
                ],
                context=TestPluginContext(),
            )
        )
        == 1
    )
    assert (
        len(
            get_autocomplete_values(
                parameter,
                [],
                depend_on_parameter_values=[
                    _.databus_base_url,
                    _.databus_document,
                    "NOTFOUND",
                    _.document_version,
                ],
                context=TestPluginContext(),
            )
        )
        == 0
    )
