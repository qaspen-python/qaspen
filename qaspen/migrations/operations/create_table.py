"""Operation when we want to create table."""
import typing

from qaspen.fields.base_field import BaseField, FieldType
from qaspen.migrations.operations.base_operation import Operation
from qaspen.querystring.querystring import QueryString


class CreateTableOperation(Operation):
    def __init__(
        self: typing.Self,
        table_name: str,
        fields: dict[str, BaseField[FieldType]],
        additional_options: dict[str, typing.Any] | None = None,
    ) -> None:
        self.table_name: typing.Final = table_name
        self.fields: typing.Final = fields
        self.additional_options: typing.Final = additional_options

    def statement(self: typing.Self) -> QueryString:
        # CONTINUE HERE
        return QueryString(
            sql_template="",
        )
