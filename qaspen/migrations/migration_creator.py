"""Class that creates migrations."""
import dataclasses
import typing

from qaspen.table.meta_table import MetaTable


@dataclasses.dataclass
class MigrationOperations:
    table_create_operations: list[str]


class MigrationCreator:
    def __init__(
        self: typing.Self,
        tables_state: dict[str, typing.Any],
    ) -> None:
        self.tables_state: typing.Final = tables_state
        self.not_abstract_tables: typing.Final = (
            MetaTable._retrieve_not_abstract_subclasses()
        )
