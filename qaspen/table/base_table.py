import typing

from qaspen.fields.base_field import BaseField
from qaspen.statements.select_statement import SelectStatement

# from qaspen.statements.update_statement import UpdateStatement
from qaspen.table.meta_table import MetaTable


class BaseTable(MetaTable):
    @classmethod
    def select(
        cls: type["BaseTable"],
        select_fields: typing.Iterable[BaseField[typing.Any]] | None = None,
    ) -> SelectStatement:
        if not select_fields:
            select_fields = cls.all_fields()
        select_statement: typing.Final[SelectStatement] = SelectStatement(
            select_fields=select_fields,
            from_table=cls,
        )
        return select_statement

    @classmethod
    def all_fields(cls: type["BaseTable"]) -> list[BaseField[typing.Any]]:
        return cls._table_meta.table_fields
