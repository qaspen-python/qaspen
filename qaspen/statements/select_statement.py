import typing
from qaspen.fields.fields import Field
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)

from qaspen.statements.sub_statements.limit_statement import LimitStatement
from qaspen.statements.combinable_statements.order_by_statement import (
    OrderBy,
    OrderByStatement,
)
from qaspen.statements.combinable_statements.where_statement import (
    WhereStatement,
)
from qaspen.table.meta_table import MetaTable

if typing.TYPE_CHECKING:
    from qaspen.statements.union_statement import (
        UnionStatement,
    )


class SelectStatement:

    def __init__(
        self: typing.Self,
        from_table: type[MetaTable],
        select_fields: typing.Iterable[Field[typing.Any]],
    ) -> None:
        self._from_table: typing.Final[type[MetaTable]] = from_table
        self._select_fields: typing.Final[
            typing.Iterable[Field[typing.Any]]
        ] = select_fields
        self.exist_prefixs: typing.Final[list[str]] = []

        self._where_statement: WhereStatement = WhereStatement()
        self._limit_statement: LimitStatement = LimitStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()

    def build_query(self) -> str:
        to_select_fields: str = ", ".join(
            [field.field_name for field in self._select_fields],
        )
        sql_statement: str = (
            f"SELECT {to_select_fields} "
            f"FROM {self._from_table._table_meta.table_name}"
        )
        sql_statement += f" {self._where_statement._build_query()}"
        sql_statement += f"{self._order_by_statement.querystring()}"
        sql_statement += f" {self._limit_statement._build_query()}"
        return sql_statement

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
        self._where_statement.where(*where_arguments)
        return self

    def limit(
        self: typing.Self,
        limit: int,
    ) -> typing.Self:
        self._limit_statement.limit(limit_number=limit)
        return self

    def order_by(
        self: typing.Self,
        field: Field[typing.Any] | None = None,
        ascending: bool = True,
        nulls_first: bool = True,
        order_by_statements: typing.Iterable[OrderBy] | None = None,
    ) -> typing.Self:
        if field:
            self._order_by_statement.order_by(
                field=field,
                ascending=ascending,
                nulls_first=nulls_first,
            )
        if order_by_statements:
            self._order_by_statement.order_by(
                order_by_statements=order_by_statements
            )
        return self

    def union(
        self,
        union_with: "SelectStatement",
        union_all: bool = False,
    ) -> "UnionStatement":
        from qaspen.statements.union_statement import (
            UnionStatement,
        )
        return UnionStatement(
            left_expression=self,
            right_expression=union_with,
            union_all=union_all,
        )

    def _to_sql_statement(self: typing.Self) -> str:
        return self.build_query()
