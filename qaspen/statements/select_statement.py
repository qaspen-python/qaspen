from collections import UserDict
import typing
from qaspen.base.sql_base import SQLSelectable
from qaspen.exceptions import OnJoinFieldsError
from qaspen.fields.base.base_field import BaseField
from qaspen.fields.fields import Field
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.combinable_statements.join_statement import (
    FullOuterJoin,
    InnerJoin,
    Join,
    JoinStatement,
    JoinType,
    LeftOuterJoin,
    RightOuterJoin
)
from qaspen.statements.statement import BaseStatement

from qaspen.statements.sub_statements.limit_statement import LimitStatement
from qaspen.statements.combinable_statements.order_by_statement import (
    OrderBy,
    OrderByStatement,
)
from qaspen.statements.combinable_statements.filter_statement import (
    FilterStatement,
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


class FieldAlias:

    def __init__(
        self: typing.Self,
        aliased_field: BaseField[typing.Any],
    ) -> None:
        self.aliased_field: typing.Final = aliased_field


class FieldAliases(UserDict[str, Field[typing.Any]]):
    def __init__(self: typing.Self):
        self.aliases: dict[str, FieldAlias] = {}
        self.last_alias_number: int = 0

    def add_annotation(
        self: typing.Self,
        field: BaseField[typing.Any],
    ) -> BaseField[typing.Any]:
        self.last_alias_number += 1

        alias: typing.Final = f"A{self.last_alias_number}"
        new_aliased_field: typing.Final = field._with_alias(alias=alias)

        self.aliases[f"A{self.last_alias_number}"] = FieldAlias(
            aliased_field=new_aliased_field,
        )
        return new_aliased_field


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

        self._filter_statement: FilterStatement = FilterStatement()
        self._limit_statement: LimitStatement = LimitStatement()
        self._offset_statement: OffsetStatement = OffsetStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()
        self._join_statement: JoinStatement = JoinStatement()
        self._field_aliases: FieldAliases = FieldAliases()

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
        self._filter_statement.where(*where_arguments)
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

    def _join_on(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
        join_type: JoinType,
    ) -> typing.Self:
        if not fields:
            raise OnJoinFieldsError(
                "You must specify fields to get from JOIN",
            )

        join_table: type[MetaTable] = (
            fields[0]
            ._field_data
            .from_table
        )

        self._join_statement.join(
            fields=fields,
            join_table=join_table,
            from_table=self._from_table,
            on=based_on,
            join_type=join_type,
        )
        return self

    def join_on(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.JOIN,
        )

    def inner_join_on(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.INNERJOIN,
        )

    def left_join_on(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.LEFTJOIN,
        )

    def right_join_on(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.RIGHTJOIN,
        )

    def full_outer_join_on(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.FULLOUTERJOIN,
        )

    def join_on_with_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> Join:
        self.join_on(
            based_on=based_on,
            fields=fields,
        )
        return self._join_statement.join_expressions[-1]

    def inner_join_on_with_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> InnerJoin:
        self.inner_join_on(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            InnerJoin,
            self._join_statement.join_expressions[-1],
        )

    def left_join_on_with_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> LeftOuterJoin:
        self.left_join_on(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            LeftOuterJoin,
            self._join_statement.join_expressions[-1],
        )

    def right_join_on_with_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> RightOuterJoin:
        self.right_join_on(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            RightOuterJoin,
            self._join_statement.join_expressions[-1],
        )

    def full_outer_join_on_with_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> FullOuterJoin:
        self.full_outer_join_on(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            FullOuterJoin,
            self._join_statement.join_expressions[-1],
        )

    def add_join(
        self: typing.Self,
        *join: Join,
    ) -> typing.Self:
        self._join_statement.add_join(
            *join,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
        fields_to_select: list[
            BaseField[typing.Any],
        ] = self.prepare_select_fields()
        to_select_fields: str = ", ".join(
            [
                field.field_name
                for field in fields_to_select
            ],
        )
        sql_querystring = QueryString(
            to_select_fields,
            self._from_table._table_meta.table_name,
            sql_template="SELECT {} FROM {}",
        )
        sql_querystring += self._join_statement.querystring()
        sql_querystring += self._filter_statement.querystring()
        sql_querystring += self._order_by_statement.querystring()
        sql_querystring += self._limit_statement.querystring()
        sql_querystring += self._offset_statement.querystring()

        return sql_querystring

    def make_sql_string(self: typing.Self) -> str:
        return str(self.querystring())

    def prepare_select_fields(
        self: typing.Self,
    ) -> list[BaseField[typing.Any]]:
        final_select_fields: typing.Final[list[BaseField[typing.Any]]] = []

        fields_to_select: list[BaseField[typing.Any]] = []
        fields_to_select.extend(self._select_fields)
        fields_to_select.extend(
            self._join_statement._retrieve_all_join_fields(),
        )

        for field in fields_to_select:
            aliased_field = self._field_aliases.add_annotation(
                field=field,
            )
            final_select_fields.append(aliased_field)

        return final_select_fields
