"""Class that creates migrations."""
import typing

from qaspen.table.meta_table import MetaTable


class MigrationCreator:
    def __init__(
        self: typing.Self,
        tables_state: dict[str, typing.Any],
    ) -> None:
        self.tables_state: typing.Final = tables_state
        self.not_abstract_tables: typing.Final = (
            MetaTable._retrieve_not_abstract_subclasses()
        )
