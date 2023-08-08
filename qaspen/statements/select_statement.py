import dataclasses
import typing
from qaspen.fields.base_field import Field
from qaspen.fields.comparisons import Where
from qaspen.statements.common.where_statement import WhereStatement
from qaspen.table.meta_table import MetaTable


@dataclasses.dataclass
class SelectStatement:
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

        self.where_statement: WhereStatement = WhereStatement()

    def build_query(self) -> str:
        to_select_fields: str = ", ".join(
            [field.field_name for field in self.select_fields],
        )
        sql_statement: str = (
            f"SELECT {to_select_fields} "
            f"FROM {self.from_table._table_meta.table_name} "
        )
        sql_statement += self.where_statement._build_where_query()
        return sql_statement

    def where(
        self: typing.Self,
        *where_arguments: Where,
    ) -> typing.Self:
        self.where_statement.where(*where_arguments)
        return self
