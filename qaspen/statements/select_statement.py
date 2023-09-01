import typing
from qaspen.base.sql_base import SQLSelectable
from qaspen.engine.base_engine import BaseEngine
from qaspen.exceptions import OnJoinFieldsError
from qaspen.fields.aliases import FieldAliases
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
        self.final_select_fields: list[BaseField[typing.Any]] = []
        self.exist_prefixes: typing.Final[list[str]] = []

        self._filter_statement: FilterStatement = FilterStatement()
        self._limit_statement: LimitStatement = LimitStatement()
        self._offset_statement: OffsetStatement = OffsetStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()
        self._join_statement: JoinStatement = JoinStatement()
        self._field_aliases: FieldAliases = FieldAliases()

        self._as_object: bool = False

    def __await__(
        self: typing.Self,
    ) -> typing.Any:
        return self._run_query().__await__()

    async def _run_query(self: typing.Self) -> typing.Any:
        if not self._from_table._table_meta.database_engine:
            raise ValueError()
        return await self._from_table._table_meta.database_engine.run_query(
            querystring=self.querystring(),
        )

    async def execute(
        self: typing.Self,
        engine: BaseEngine[typing.Any],
        as_object: bool = False,
    ) -> typing.Any:
        from qaspen.query_result import QueryResult
        raw_query_result: tuple[
            tuple[typing.Any, ...], ...,
        ] = await engine.run_query(
            querystring=self.querystring(),
        )

        query_result: QueryResult = QueryResult(
            from_table=self._from_table,  # type: ignore
            query_result=raw_query_result,
            aliases=self._field_aliases,
        )

        if self._as_object or as_object:
            pass

        return query_result.as_list()

    def as_object(self: typing.Self) -> typing.Self:
        self._as_object = True
        return self

    def _result_as_list(
        self: typing.Self,
        query_result: tuple[tuple[typing.Any, ...], ...],
    ) -> dict[str, typing.Any]:
        result_dict: dict[str, list[dict[str, typing.Any]]] = {
            field.aliased_field.table_name: []
            for field in self._field_aliases.values()
        }

        for single_query_result in query_result:
            zip_expression = zip(
                single_query_result,
                self._field_aliases.values(),
            )

            for values_list in result_dict.values():
                values_list.append({})

            for single_query_result, field in zip_expression:
                table_list = result_dict[field.aliased_field.table_name]
                last_record_dict = table_list[-1]

                last_record_dict[
                    f"_{field.aliased_field.field_name_clear}"
                ] = single_query_result

        return result_dict

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
        ] = self._prepare_select_fields()
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

    def _prepare_select_fields(
        self: typing.Self,
    ) -> list[BaseField[typing.Any]]:
        if self.final_select_fields:
            return self.final_select_fields
        final_select_fields: typing.Final[list[BaseField[typing.Any]]] = []

        fields_to_select: list[BaseField[typing.Any]] = []
        fields_to_select.extend(self._select_fields)
        fields_to_select.extend(
            self._join_statement._retrieve_all_join_fields(),
        )

        for field in fields_to_select:
            aliased_field = self._field_aliases.add_alias(
                field=field,
            )
            final_select_fields.append(aliased_field)

        self.final_select_fields = final_select_fields
        return final_select_fields
