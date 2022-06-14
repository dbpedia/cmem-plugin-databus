"""Random values workflow plugin module"""
import uuid
from secrets import token_urlsafe

from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities, Entity, EntitySchema, EntityPath,
)
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin


@Plugin(
    label="Random Values (databus)",
    description="Generates random values of X rows a Y values.",
    documentation="""
This example workflow operator generates random values.

The values are generated in X rows a Y values. Both parameter can be specified:

- 'number_of_entities': How many rows do you need.
- 'number_of_values': How many values per row do you need.
""",
    parameters=[
        PluginParameter(
            name="number_of_entities",
            label="Entities (Rows)",
            description="How many rows will be created per run.",
            default_value="10",
        ),
        PluginParameter(
            name="number_of_values",
            label="Values (Columns)",
            description="How many values are created per entity / row.",
            default_value="5"
        )
    ]
)
class DollyPlugin(WorkflowPlugin):
    """Example Workflow Plugin: Random Values"""

    def __init__(
            self,
            number_of_entities: int = 10,
            number_of_values: int = 5
    ) -> None:
        if number_of_entities < 1:
            raise ValueError(
                "Entities (Rows) needs to be a positive integer."
            )
        if number_of_values < 1:
            raise ValueError(
                "Values (Columns) needs to be a positive integer."
            )
        self.number_of_entities = number_of_entities
        self.number_of_values = number_of_values

    def execute(self, inputs=()) -> Entities:
        self.log.info("Start creating random values.")
        self.log.info(f"Config length: {len(self.config.get())}")
        value_counter = 0
        entities = []
        for _ in range(self.number_of_entities):
            entity_uri = f"urn:uuid:{str(uuid.uuid4())}"
            values = []
            for _ in range(self.number_of_values):
                values.append([token_urlsafe(16)])
                value_counter += 1
            entities.append(
                Entity(
                    uri=entity_uri,
                    values=values
                )
            )
        paths = []
        for path_no in range(self.number_of_values):
            path_uri = f"https://example.org/vocab/RandomValuePath/{path_no}"
            paths.append(
                EntityPath(
                    path=path_uri
                )
            )
        schema = EntitySchema(
            type_uri="https://example.org/vocab/RandomValueRow",
            paths=paths,
        )
        self.log.info(f"Happy to serve {value_counter} random values.")
        return Entities(entities=entities, schema=schema)
