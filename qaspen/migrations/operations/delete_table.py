import typing

from qaspen.migrations.operations.base_operation import Operation
from qaspen.querystring.querystring import QueryString


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
