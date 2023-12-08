from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generator,
    Generic,
    Iterable,
    Optional,
    Sequence,
    overload,
)

from qaspen.aggregate_functions.base import AggFunction
from qaspen.fields.aliases import FieldAliases
from qaspen.fields.base import Field
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import (
    CommaSeparatedQueryString,
    QueryString,
)
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.filter_statement import (
    FilterStatement,
)
from qaspen.statements.combinable_statements.join_statement import (
    JoinStatement,
    JoinType,
)
from qaspen.statements.combinable_statements.order_by_statement import (
    OrderByStatement,
)
from qaspen.statements.statement import BaseStatement
from qaspen.statements.statement_result.select_statement_result import (
    SelectStatementResult,
)
from qaspen.statements.sub_statements.limit_statement import LimitStatement
from qaspen.statements.sub_statements.offset_statement import OffsetStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction
    from qaspen.clauses.order_by import OrderBy
    from qaspen.statements.combinable_statements.combinations import (
        CombinableExpression,
    )
    from qaspen.statements.exists_statement import ExistsStatement
    from qaspen.statements.intersect_statement import IntersectStatement
    from qaspen.statements.union_statement import UnionStatement
    from qaspen.table.base_table import BaseTable


class SelectStatement(
    BaseStatement,
    Executable[Optional[SelectStatementResult]],
    Generic[FromTable],
):
    """Main entry point for all SELECT queries.

    You shouldn't create instance of this class by yourself,
    instead you must use Table class.

    Example:
    -------
    ```
    class Buns(BaseTable, table_name="buns"):
        name: VarCharField = VarCharField()


    select_statement: SelectStatement = Buns.select()
    ```
    """

    def __init__(
        self: Self,
        from_table: type[FromTable],
        select_objects: Sequence[Field[Any] | AggFunction],
    ) -> None:
        self._from_table: Final[type[FromTable]] = from_table

        self._select_fields = []
        self._select_agg_functions: Sequence[AggFunction] = []

        for select_object in select_objects:
            if isinstance(select_object, Field):
                self._select_fields.append(select_object)
            elif isinstance(select_object, AggFunction):
                self._select_agg_functions.append(
                    select_object,
                )

        self.final_select_fields: list[Field[Any]] = []
        self.exist_prefixes: Final[list[str]] = []

        self._filter_statement: FilterStatement = FilterStatement()
        self._limit_statement: LimitStatement = LimitStatement()
        self._offset_statement: OffsetStatement = OffsetStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()
        self._join_statement: JoinStatement = JoinStatement()
        self._field_aliases: FieldAliases = FieldAliases()

    def __await__(
        self: Self,
    ) -> Generator[None, None, SelectStatementResult]:
        """SelectStatement can be awaited.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()

        async def main() -> None:
            list_of_buns = await Buns.select()
            print(list_of_buns)
        ```
        """
        engine: Final = self._from_table._table_meta.database_engine
        if not engine:
            engine_err_msg: Final = "There is no database engine."
            raise AttributeError(engine_err_msg)

        return self.execute(engine=engine).__await__()

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> SelectStatementResult:
        """Execute select statement.

        This is manual execution.
        You can pass specific engine.

        ### Parameters
        - `engine`: subclass of BaseEngine.
        database response or not.

        ### Returns
        `SelectStatementResult`
        """
        raw_query_result: list[dict[str, Any]] = await engine.execute(
            querystring=self.querystring().build(),
            querystring_parameters=[],
            fetch_results=True,
        )

        return SelectStatementResult(
            engine_result=raw_query_result,
        )

    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> SelectStatementResult:
        """Execute statement inside a transaction context.

        This is manual execution.
        You can pass specific transaction.
        IMPORTANT: To commit the changes, with this way of execution
        it's necessary to manually call `await transaction.commit()`.

        ### Parameters:
        - `transaction`: running transaction.
        database response or not.

        ### Returns
        `SelectStatementResult`
        """
        raw_query_result: list[dict[str, Any]] = await transaction.execute(
            querystring=self.querystring().build(),
            querystring_parameters=[],
            fetch_results=True,
        )

        return SelectStatementResult(
            engine_result=raw_query_result,
        )

    def where(
        self: Self,
        *where_arguments: CombinableExpression,
    ) -> Self:
        """Add where clause to the statement.

        It's possible to use this method as many times as you want.
        If you use `where` more than one time, clauses will be connected
        with `AND` operator.

        Fields have different methods for the comparison.
        Also, you can pass the combination of the `where` clauses.

        Below you will see easy and advanced examples.

        One Where Example:
        ------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()

        statement = Buns.select().where(
            Buns.name == "Tasty",
        )
        ```
        Combination Where Example:
        ------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()
            example_field: VarCharField = VarCharField()

        statement = Buns.select().where(
            (Buns.name == "Tasty")
            & (Buns.description != "Not Tasty")
            | Buns.example_field.eq("Real Example"))
        )
        ```
        In this statement `&` is AND, `|` is OR.
        """
        self._filter_statement.add_filter(*where_arguments)
        return self

    def limit(
        self: Self,
        limit: int,
    ) -> Self:
        """Set limit to the query.

        If you use this method more than 1 time, previous result
        will be overridden.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        statement = Buns.select().limit(limit=10)

        # if you will use limit() again, previous number
        # will be overridden.

        statement = statement.limit(limit=30)

        # not limit is LIMIT 30.
        ```
        """
        self._limit_statement.limit(limit_number=limit)
        return self

    def offset(
        self: Self,
        offset: int,
    ) -> Self:
        """Set offset to the query.

        If you use this method more than 1 time, previous result
        will be overridden.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        statement = Buns.select().offset(offset=10)

        # if you will use offset() again, previous number
        # will be overridden.

        statement = statement.offset(offset=30)

        # not limit is OFFSET 30.
        ```
        """
        self._offset_statement.offset(offset_number=offset)
        return self

    def limit_offset(
        self: Self,
        limit: int,
        offset: int,
    ) -> Self:
        """Set offset and limit at the same time.

        See `limit()` and `offset()` methods description.
        """
        self._limit_statement.limit(limit_number=limit)
        self._offset_statement.offset(offset_number=offset)
        return self

    def order_by(
        self: Self,
        field: Field[Any] | None = None,
        ascending: bool = True,
        nulls_first: bool = True,
        order_bys: Iterable[OrderBy] | None = None,
    ) -> Self:
        """Set ORDER BY.

        You can specify `field`, `ascending` and `nulls_first`
        parameters and/or pass `order_bys` with OrderBy instances.

        ### Parameters
        :param field: subclass of BaseField.
        :param ascending: if True then `ASC` else `DESC`.
        :param nulls_first: if True then `NULLS FIRST` else `NULLS LAST`.
        :param order_bys: list of instances of order by.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        # first variant
        statement = Buns.select().order_by(
            field=Buns.name,
            # it's not necessary, but you can specify.
            ascending=False,  # by default its `True`
            nulls_first=False,  # by default its `True`
        )

        # second variant
        order_bys = [
            OrderBy(
                field=Buns.name,
                # it's not necessary, but you can specify.
                ascending=False,  # by default its `True`
                nulls_first=False,  # by default its `True`
            ),
            OrderBy(
                field=Buns.surname,
            ),
        ]
        statement = Buns.select().order_by(
            order_bys=order_bys,
        )
        ```
        """
        if field:
            self._order_by_statement.order_by(
                field=field,
                ascending=ascending,
                nulls_first=nulls_first,
            )
        if order_bys:
            self._order_by_statement.order_by(
                order_by_expressions=order_bys,
            )
        return self

    @overload
    def union(
        self: Self,
        union_with: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> UnionStatement[FromTable]:
        ...  # pragma: no cover

    @overload
    def union(
        self: Self,
        union_with: SelectStatement[Any],
        union_all: bool = False,
    ) -> UnionStatement[FromTable]:
        ...  # pragma: no cover

    def union(
        self: Self,
        union_with: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> UnionStatement[FromTable]:
        """Create union statement.

        Combines two `SelectStatement`'s and creates
        `UnionStatement`.

        ### Parameters
        :param union_with: `SelectStatement` for union.
        :param union_all: if True than `UNION ALL` else `UNION`.

        ### Returns
        :returns: `UnionStatement`.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        statement_1 = Buns.select()
        statement_2 = Buns.select()
        union = statement_1.union(
            union_with=statement_2,
            union_all=False,
        )
        ```
        """
        from qaspen.statements.union_statement import UnionStatement

        return UnionStatement[FromTable](
            left_expression=self,
            right_expression=union_with,
            union_all=union_all,
        )

    def intersect(
        self: Self,
        intersect_with: SelectStatement[FromTable],
    ) -> IntersectStatement:
        """Create intersect statement.

        Combines two `SelectStatement`'s and creates
        `IntersectStatement`

        ### Parameters
        :param intersect_with: `SelectStatement` for intersect.

        ### Returns
        :returns: `IntersectStatement`.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        statement_1 = Buns.select()
        statement_2 = Buns.select()
        intersect = statement_1.intersect(
            intersect_with=statement_2,
        )
        ```
        """
        from qaspen.statements.intersect_statement import IntersectStatement

        return IntersectStatement(
            left_expression=self,
            right_expression=intersect_with,
        )

    def exists(self: Self) -> ExistsStatement:
        """Create exists statement.

        Create exists statement that return True or False.

        ### Returns
        :returns: `ExistsStatement`.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        exists_statement = Buns.select().where(
            Buns.name == "CallMe",
        ).exists()
        ```
        """
        from qaspen.statements.exists_statement import ExistsStatement

        self._select_fields = []
        for join in self._join_statement.join_expressions:
            join._fields = []
        return ExistsStatement(
            select_statement=self,
        )

    def join(
        self: Self,
        join_table: type[BaseTable],
        based_on: CombinableExpression,
    ) -> Self:
        """Add `JOIN` to the SelectStatement.

        Set `ON` condition, it can be a combination.

        ### Parameters
            - `join_table`: table in JOIN.
            - `based_on`: Field's comparisons.

        ### Returns
        `self`

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()

        statement = (
            Buns
            .select(
                Cookies.filling,
                Cookies.topping,
            )
            .join(
                join_table=Cookies,
                based_on=Buns.name == Cookies.bun_name,
            )
        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            join_table=join_table,
            based_on=based_on,
            join_type=JoinType.JOIN,
        )

    def inner_join(
        self: Self,
        join_table: type[BaseTable],
        based_on: CombinableExpression,
    ) -> Self:
        """Add `INNER JOIN` to the SelectStatement.

        Set `ON` condition, it can be a combination.

        ### Parameters
        - `join_table`: table in JOIN.
        - `based_on`: Field's comparisons.

        ### Returns
        `self`

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()

        statement = (
            Buns
            .select(
                Cookies.filling,
                Cookies.topping,
            )
            .inner_join(
                join_table=Cookies,
                based_on=Buns.name == Cookies.bun_name,
            )
        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            join_table=join_table,
            based_on=based_on,
            join_type=JoinType.INNERJOIN,
        )

    def left_join(
        self: Self,
        join_table: type[BaseTable],
        based_on: CombinableExpression,
    ) -> Self:
        """Add `LEFT JOIN` to the SelectStatement.

        Set `ON` condition, it can be a combination.

        ### Parameters
        - `join_table`: table in JOIN.
        - `based_on`: Field's comparisons.

        ### Returns
        `self`

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()

        statement = (
            Buns
            .select(
                Cookies.filling,
                Cookies.topping
            )
            .left_join(
                join_table=Cookies,
                based_on=Buns.name == Cookies.bun_name,
            )

        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            join_table=join_table,
            based_on=based_on,
            join_type=JoinType.LEFTJOIN,
        )

    def right_join(
        self: Self,
        join_table: type[BaseTable],
        based_on: CombinableExpression,
    ) -> Self:
        """Add `RIGHT JOIN` to the SelectStatement.

        Set `ON` condition, it can be a combination.

        ### Parameters
        - `join_table`: table in JOIN.
        - `based_on`: Field's comparisons.

        ### Returns
        `self`

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()

        statement = (
            Buns
            .select(
                Cookies.filling,
                Cookies.topping,
            )
            .right_join(
                join_table=Cookies,
                based_on=Buns.name == Cookies.bun_name,
            )
        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            join_table=join_table,
            based_on=based_on,
            join_type=JoinType.RIGHTJOIN,
        )

    def full_outer_join(
        self: Self,
        join_table: type[BaseTable],
        based_on: CombinableExpression,
    ) -> Self:
        """Add `FULL OUTER JOIN` to the SelectStatement.

        You can specify what fields you want to get from
        the joined table.
        And set `ON` condition, it can be a combination.

        ### Parameters
        - `join_table`: table in JOIN.
        - `based_on`: Field's comparisons.

        ### Returns
        `self`

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Cookies(BaseTable, table_name="cookies"):
            bun_name: VarCharField = VarCharField()
            filling: VarCharField = VarCharField()
            topping: VarCharField = VarCharField()


        statement = (
            Buns
            .select(
                Cookies.filling,
                Cookies.topping,
            )
            .full_outer_join(
                join_table=Cookies,
                based_on=Buns.name == Cookies.bun_name,
            )
        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            join_table=join_table,
            based_on=based_on,
            join_type=JoinType.FULLOUTERJOIN,
        )

    def querystring(self: Self) -> QueryString:
        """Build querystring.

        Can be transformed into the SQL query with `str` method.

        :returns: QueryString.
        """
        sql_querystring = self._select_from()
        sql_querystring += self._join_statement.querystring()
        sql_querystring += self._filter_statement.querystring()
        sql_querystring += self._order_by_statement.querystring()
        sql_querystring += self._limit_statement.querystring()
        sql_querystring += self._offset_statement.querystring()

        return sql_querystring

    def _join_on(
        self: Self,
        join_table: type[BaseTable],
        based_on: CombinableExpression,
        join_type: JoinType,
    ) -> Self:
        self._join_statement.join(
            join_table=join_table,
            from_table=self._from_table,
            on=based_on,
            join_type=join_type,
        )
        return self

    def _select_from(self: Self) -> QueryString:
        """Build `QueryString`.

        Firstly, we need to select fields from main table
        and from joins (if they exist).
        Then we need to add in select all
        aggregate functions.

        At the end, if we have alias for the table,
        we use it in AS operator.

        ### Returns:
        new `QueryString` for SELECT FROM.
        """
        prepared_fields = self._prepare_fields()

        to_select_fields_qs = CommaSeparatedQueryString(
            *[
                QueryString(
                    field.field_name,
                    sql_template="{}",
                )
                for field in prepared_fields
            ],
            sql_template=", ".join(["{}"] * len(prepared_fields)),
        )
        to_select_agg_func_qs = CommaSeparatedQueryString(
            *[
                agg_func.querystring()
                for agg_func in self._select_agg_functions
            ],
            sql_template=", ".join(["{}"] * len(self._select_agg_functions)),
        )

        final_to_select: QueryString | None = None
        if to_select_fields_qs.template_arguments:
            final_to_select = to_select_fields_qs
        if to_select_agg_func_qs.template_arguments:
            if final_to_select:
                final_to_select = final_to_select + to_select_agg_func_qs
            else:
                final_to_select = to_select_agg_func_qs

        if self._from_table._table_meta.alias:
            return QueryString(
                final_to_select or "1",
                self._from_table.schemed_original_table_name(),
                self._from_table._table_meta.alias,
                sql_template="SELECT {} FROM {} AS {}",
            )

        return QueryString(
            final_to_select or "1",
            self._from_table.schemed_original_table_name(),
            sql_template="SELECT {} FROM {}",
        )

    def _prepare_fields(
        self: Self,
    ) -> list[Field[Any]]:
        """Prepare fields.

        If we've already prepared fields, return them.

        We need to get all fields to select,
        this is fields from main table and fields from joins.

        After we get all fields, we add alias to them.

        ### Returns:
        list of aliased fields.
        """
        if self.final_select_fields:
            return self.final_select_fields
        final_select_fields: Final[list[Field[Any]]] = []

        fields_to_select: list[Field[Any]] = []
        fields_to_select.extend(self._prepare_select_fields())
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

    def _prepare_select_fields(
        self: Self,
    ) -> list[Field[Any]]:
        """Prepare select fields.

        We split all select fields into two categories,
        fields from main table and fields from joins.

        We return fields from main table.

        Fields from joins we add to the join expressions,
        because join statement has it's own logic for
        fields processing.

        ### Returns:
        list of select fields from main table.
        """
        fields_from_original_table: list[Field[Any]] = []
        fields_from_joined_table: dict[
            str,
            list[Field[Any]],
        ] = {}

        for field in self._select_fields:
            is_original = field._field_data.from_table == self._from_table
            if is_original:
                fields_from_original_table.append(
                    field,
                )
            else:
                fields_from_joined_table.setdefault(
                    field._field_data.from_table.original_table_name(),
                    [],
                ).append(field)

        for join_expr in self._join_statement.join_expressions:
            join_fields = fields_from_joined_table.get(
                join_expr._join_table.original_table_name(),
            )
            if not join_fields:
                continue
            join_expr.add_fields(join_fields)

        return fields_from_original_table
