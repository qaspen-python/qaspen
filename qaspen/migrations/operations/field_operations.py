import dataclasses
import typing

from qaspen.fields.base_field import FieldData, FieldType
from qaspen.migrations.operations.base_operation import Operation
from qaspen.querystring.querystring import QueryString


@dataclasses.dataclass
class CreateFieldOperation(Operation, FieldData[FieldType]):
    def statement(self: typing.Self) -> QueryString:
        return QueryString(
            sql_template="",
        )

    def migration_string(self: typing.Self) -> str:
        return "CreateFieldOperation()"


class DeleteTableOperation(Operation):
    def __init__(
        self: typing.Self,
        table_name: str,
    ) -> None:
        self.table_name: typing.Final = table_name

    def statement(self: typing.Self) -> QueryString:
        return QueryString(
            self.table_name,
            sql_template="DROP TABLE {}",
        )
