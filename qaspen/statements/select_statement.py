import dataclasses
import typing
from qaspen.fields.base_field import Field
from qaspen.statements.common.where_statement import WhereStatementMixin
from qaspen.table.meta_table import MetaTable


# @dataclasses.dataclass
class SelectStatement(WhereStatementMixin):
    def __init__(
        self: typing.Self,
        from_table: type[MetaTable],
        select_fields: list[Field[typing.Any]],
    ) -> None:
        self.from_table: typing.Final[type[MetaTable]] = from_table
        self.select_fields: typing.Final[
            list[Field[typing.Any]]
        ] = select_fields
        self.exist_prefixs: typing.Final[list[str]] = []

    def build_query(self) -> str:
        to_select_fields: str = ", ".join(
            [field.field_name for field in self.select_fields],
        )
        sql_statement: str = (
            f"SELECT {to_select_fields} "
            f"FROM {self.from_table._table_meta.table_name} "
        )
        sql_statement += self._build_where_query()
        return sql_statement
