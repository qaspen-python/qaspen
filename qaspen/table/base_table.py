import typing

from qaspen.fields.base_field import BaseField
from qaspen.fields.fields import Field
from qaspen.statements.select_statement import SelectStatement

# from qaspen.statements.update_statement import UpdateStatement
from qaspen.table.meta_table import MetaTable


class BaseTable(MetaTable, abstract=True):
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

    @classmethod
    def retrieve_field(
        cls: type["BaseTable"],
        field_name: str,
    ) -> Field[typing.Any]:
        try:
            return typing.cast(
                Field[typing.Any],
                cls.__dict__[field_name],
            )
        except LookupError:
            raise AttributeError(
                f"Table `{cls.__name__}` doesn't have `{field_name}` field",
            )

    @classmethod
    def table_name(cls: type["BaseTable"]) -> str:
        return cls._table_meta.table_name
