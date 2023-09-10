import io
from dataclasses import dataclass

import pytest
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import resource_exist, \
    create_resource

from cmem_plugin_databus.loader import SimpleDatabusLoadingPlugin, DatabusSearch, \
    ResourceParameterType, FacetSearch, DatabusFile
from .utils import needs_cmem, TestExecutionContext, TestPluginContext

DATABUS_BASE_URL = "https://databus.dbpedia.org"
DATABUS_DOCUMENT = f"{DATABUS_BASE_URL}/cmempydeveloper/CorporateMemory/Documentation"
DOCUMENT_VERSION = "23.01"
DOCUMENT_FORMAT = "md"
DATABUS_FILE = (
    "https://databus.dbpedia.org/cmempydeveloper/CorporateMemory/"
    f"Documentation/{DOCUMENT_VERSION}/Documentation.md"
)


def get_autocomplete_values(
        parameter,
        query_terms,
        depend_on_parameter_values,
        context
):
    """get autocomplete values"""
    if depend_on_parameter_values is None:
        depend_on_parameter_values = []
    return [
        x.value
        for x in parameter.autocomplete(
            query_terms=query_terms,
            depend_on_parameter_values=depend_on_parameter_values,
            context=context
        )
    ]


@pytest.fixture(name="project")
def project():
    """Provides the DI build project incl. assets."""
    project_name = 'databus_sample_project'
    make_new_project(project_name)
    yield project_name
    delete_project(project_name)


@pytest.fixture(name="resource")
def resource(project):
    """setup json resource"""
    _resource_name = "sample_test.txt"
    create_resource(
        project_name=project,
        resource_name=_resource_name,
        file_resource=io.StringIO("SAMPLE CONTENT"),
        replace=True
    )

    @dataclass
    class FixtureDate:
        """fixture dataclass"""
        project_name = project
        resource_name = _resource_name

    _ = FixtureDate()
    yield _


@needs_cmem
def test_databus_load(project):
    resource_name = "upload_readme.md"
    databus_load = SimpleDatabusLoadingPlugin(
        databus_base_url=DATABUS_BASE_URL,
        databus_artifact="",
        artifact_format="",
        artifact_version="",
        databus_file_id="https://databus.dbpedia.org/cmempydeveloper/CorporateMemory"
                        "/Documentation/23.02/Documentation.md",
        target_file=resource_name
    )
    databus_load.execute(
        inputs=(),
        context=TestExecutionContext(project_id=project)
    )
    assert resource_exist(project_name=project, resource_name=resource_name)


@needs_cmem
def test_databus_search_auto_complete():
    parameter = DatabusSearch()
    assert '' in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[],
        context=TestPluginContext())

    assert len(get_autocomplete_values(
        parameter,
        ['NOTFOUND'],
        depend_on_parameter_values=[DATABUS_BASE_URL],
        context=TestPluginContext())) == 0


@needs_cmem
def test_resource_parameter_type_completion(resource):
    """test resource parameter type completion"""
    project_name = resource.project_name
    resource_name = resource.resource_name
    parameter = ResourceParameterType()
    context = TestPluginContext(project_name)
    assert resource_name in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[],
        context=context
    )
    new_resource_name = "lkshfkdsjfhsd"
    assert len(
        get_autocomplete_values(
            parameter,
            [new_resource_name],
            depend_on_parameter_values=[],
            context=context
        )
    ) == 1
    assert new_resource_name in get_autocomplete_values(
        parameter,
        [new_resource_name],
        depend_on_parameter_values=[],
        context=context
    )


@needs_cmem
def test_facet_search_auto_complete():
    parameter = FacetSearch(facet_option='format')
    assert DOCUMENT_FORMAT in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[
            DATABUS_BASE_URL,
            DATABUS_DOCUMENT
        ],
        context=TestPluginContext())

    assert len(get_autocomplete_values(
        parameter,
        ['NOTFOUND'],
        depend_on_parameter_values=[
            DATABUS_BASE_URL,
            DATABUS_DOCUMENT
        ],
        context=TestPluginContext())) == 0

    parameter = FacetSearch(facet_option='version')
    assert DOCUMENT_VERSION in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[
            DATABUS_BASE_URL,
            DATABUS_DOCUMENT
        ],
        context=TestPluginContext())
    assert DOCUMENT_VERSION not in get_autocomplete_values(
        parameter,
        ["23.02"],
        depend_on_parameter_values=[
            DATABUS_BASE_URL,
            DATABUS_DOCUMENT
        ],
        context=TestPluginContext())
    assert len(
        get_autocomplete_values(
            parameter,
            [],
            depend_on_parameter_values=[
                DATABUS_BASE_URL,
                DATABUS_DOCUMENT
            ],
            context=TestPluginContext()
        )
    ) == 2
    assert len(get_autocomplete_values(
        parameter,
        ['NOTFOUND'],
        depend_on_parameter_values=[
            DATABUS_BASE_URL,
            DATABUS_DOCUMENT
        ],
        context=TestPluginContext())) == 0


@needs_cmem
def test_databus_file_auto_complete():
    parameter = DatabusFile()
    assert DATABUS_FILE in get_autocomplete_values(
        parameter,
        [],
        depend_on_parameter_values=[
            DATABUS_BASE_URL,
            DATABUS_DOCUMENT,
            DOCUMENT_FORMAT,
            DOCUMENT_VERSION
        ],
        context=TestPluginContext())
    assert len(
        get_autocomplete_values(
            parameter,
            [],
            depend_on_parameter_values=[
                DATABUS_BASE_URL,
                DATABUS_DOCUMENT,
                DOCUMENT_FORMAT,
                DOCUMENT_VERSION
            ],
            context=TestPluginContext()
        )
    ) == 1
    assert len(
        get_autocomplete_values(
            parameter,
            ["ADSLASD"],
            depend_on_parameter_values=[
                DATABUS_BASE_URL,
                DATABUS_DOCUMENT,
                DOCUMENT_FORMAT,
                DOCUMENT_VERSION
            ],
            context=TestPluginContext()
        )
    ) == 1
    assert len(
        get_autocomplete_values(
            parameter,
            [],
            depend_on_parameter_values=[
                DATABUS_BASE_URL,
                DATABUS_DOCUMENT,
                "NOTFOUND",
                DOCUMENT_VERSION
            ],
            context=TestPluginContext()
        )
    ) == 0
