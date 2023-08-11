import dataclasses
import typing
from qaspen.fields.fields import Field
from qaspen.fields.comparisons import CombinableExpression
from qaspen.statements.sub_statements.limit_statement import LimitStatement
from qaspen.statements.sub_statements.order_by_statement import (
    OrderBy,
    OrderByStatement,
)
from qaspen.statements.sub_statements.where_statement import WhereStatement
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
        self.limit_statement: LimitStatement = LimitStatement()
        self.order_by_statement: OrderByStatement = OrderByStatement()

    def build_query(self) -> str:
        to_select_fields: str = ", ".join(
            [field.field_name for field in self.select_fields],
        )
        sql_statement: str = (
            f"SELECT {to_select_fields} "
            f"FROM {self.from_table._table_meta.table_name} "
        )
        sql_statement += self.where_statement._build_query()
        sql_statement += self.order_by_statement._build_query()
        sql_statement += self.limit_statement._build_query()
        return sql_statement

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
        self.where_statement.where(*where_arguments)
        return self

    def limit(
        self: typing.Self,
        limit: int,
    ) -> typing.Self:
        self.limit_statement.limit(limit_number=limit)
        return self

    def order_by(
        self: typing.Self,
        field: Field[typing.Any] | None = None,
        ascending: bool = True,
        nulls_first: bool = True,
        order_by_statements: typing.Iterable[OrderBy] | None = None,
    ) -> typing.Self:
        # print(field)
        if field:
            self.order_by_statement.order_by(
                field=field,
                ascending=ascending,
                nulls_first=nulls_first,
            )
        if order_by_statements:
            self.order_by_statement.order_by(
                order_by_statements=order_by_statements
            )
        return self
