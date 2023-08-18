import typing
from qaspen.fields.base.base_field import BaseField
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

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
    from qaspen.statements.intersect_statement import (
        IntersectStatement,
    )


class SelectStatement(BaseStatement):
    def __init__(
        self: typing.Self,
        from_table: type[MetaTable],
        select_fields: typing.Iterable[BaseField[typing.Any]],
    ) -> None:
        self._from_table: typing.Final[type[MetaTable]] = from_table
        self._select_fields: typing.Final[
            typing.Iterable[BaseField[typing.Any]]
        ] = select_fields
        self.exist_prefixes: typing.Final[list[str]] = []

        self._where_statement: WhereStatement = WhereStatement()
        self._limit_statement: LimitStatement = LimitStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()

    def build_query(self) -> str:
        return str(self.querystring())

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
        field: BaseField[typing.Any] | None = None,
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
        self: typing.Self,
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

    def intersect(
        self: typing.Self,
        intersect_with: "SelectStatement",
    ) -> "IntersectStatement":
        from qaspen.statements.intersect_statement import (
            IntersectStatement,
        )
        return IntersectStatement(
            left_expression=self,
            right_expression=intersect_with,
        )

    def querystring(self: typing.Self) -> QueryString:
        to_select_fields: str = ", ".join(
            [field.field_name for field in self._select_fields],
        )
        sql_statement = QueryString(
            to_select_fields,
            self._from_table._table_meta.table_name,
            sql_template="SELECT {} FROM {}",
        )
        sql_statement += self._where_statement.querystring()
        sql_statement += self._order_by_statement.querystring()
        sql_statement += self._limit_statement.querystring()

        return sql_statement
