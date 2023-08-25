import typing
from qaspen.base.sql_base import SQLSelectable
from qaspen.fields.base.base_field import BaseField
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.combinable_statements.join_statement import (
    JoinStatement
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
from qaspen.statements.sub_statements.offset_statement import OffsetStatement
from qaspen.table.meta_table import MetaTable

if typing.TYPE_CHECKING:
    from qaspen.statements.union_statement import (
        UnionStatement,
    )
    from qaspen.statements.intersect_statement import (
        IntersectStatement,
    )
    from qaspen.statements.exists_statement import (
        ExistsStatement,
    )


class SelectStatement(BaseStatement, SQLSelectable):
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
        self._offset_statement: OffsetStatement = OffsetStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()
        self._join_statements: list[JoinStatement] = []

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

    def offset(
        self: typing.Self,
        offset: int,
    ) -> typing.Self:
        self._offset_statement.set_offset(offset_number=offset)
        return self

    def limit_offset(
        self: typing.Self,
        limit: int,
        offset: int,
    ) -> typing.Self:
        self._limit_statement.limit(limit_number=limit)
        self._offset_statement.set_offset(offset_number=offset)
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

    def exists(self: typing.Self) -> "ExistsStatement":
        from qaspen.statements.exists_statement import (
            ExistsStatement,
        )
        return ExistsStatement(
            select_statement=self,
        )

    def join(
        self: typing.Self,
        fields_to_join: list[BaseField[typing.Any]],
        based_on: CombinableExpression,
    ) -> typing.Self:
        if not fields_to_join:
            raise ValueError()

        join_table: type[MetaTable] = (
            fields_to_join[0]
            ._field_data
            .from_table
        )

        join_alias: str = (
            f"{self._from_table._table_name()}_"
            f"{join_table._table_name()}_"
            f"{len(self._join_statements) + 1}"
        )

        join_fields = [
            field._with_prefix(join_alias)
            for field in fields_to_join
        ]

        self._join_statements.append(
            JoinStatement(
                fields=join_fields,
                join_table=join_table,
                from_table=self._from_table,
                on=based_on,
                alias=join_alias,
            )
        )
        typing.SupportsIndex
        return self

    def querystring(self: typing.Self) -> QueryString:
        fields_to_select: list[BaseField[typing.Any]] = []
        fields_to_select.extend(self._select_fields)
        fields_to_select.extend(self._build_join_fields())
        to_select_fields: str = ", ".join(
            [
                field.field_name_with_prefix
                for field in fields_to_select
            ],
        )
        sql_querystring = QueryString(
            to_select_fields,
            self._from_table._table_meta.table_name,
            sql_template="SELECT {} FROM {}",
        )
        for join_statement in self._join_statements:
            sql_querystring += join_statement.querystring()
        sql_querystring += self._where_statement.querystring()
        sql_querystring += self._order_by_statement.querystring()
        sql_querystring += self._limit_statement.querystring()
        sql_querystring += self._offset_statement.querystring()

        return sql_querystring

    def make_sql_string(self: typing.Self) -> str:
        return str(self.querystring())

    def _build_join_fields(self: typing.Self) -> list[BaseField[typing.Any]]:
        fields_to_join: list[BaseField[typing.Any]] = []
        for join_statement in self._join_statements:
            fields_to_join.extend(
                join_statement._fields,
            )

        return fields_to_join
