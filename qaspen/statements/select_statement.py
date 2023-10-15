import typing

from qaspen.base.sql_base import SQLSelectable
from qaspen.engine.base import BaseEngine
from qaspen.exceptions import OnJoinFieldsError
from qaspen.fields.aliases import FieldAliases
from qaspen.fields.base_field import BaseField
from qaspen.fields.fields import Field
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import ObjectExecutable
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.combinable_statements.filter_statement import (
    FilterStatement,
)
from qaspen.statements.combinable_statements.join_statement import (
    FullOuterJoin,
    InnerJoin,
    Join,
    JoinStatement,
    JoinType,
    LeftOuterJoin,
    RightOuterJoin,
)
from qaspen.statements.combinable_statements.order_by_statement import (
    OrderBy,
    OrderByStatement,
)
from qaspen.statements.statement import BaseStatement
from qaspen.statements.sub_statements.limit_statement import LimitStatement
from qaspen.statements.sub_statements.offset_statement import OffsetStatement

if typing.TYPE_CHECKING:
    from qaspen.statements.exists_statement import ExistsStatement
    from qaspen.statements.intersect_statement import IntersectStatement
    from qaspen.statements.statement_result.select_result import (
        SelectStatementResult,
    )
    from qaspen.statements.union_statement import UnionStatement
    from qaspen.table.base_table import BaseTable


class SelectStatement(
    BaseStatement,
    SQLSelectable,
    ObjectExecutable["SelectStatementResult[FromTable]"],
    typing.Generic[FromTable],
):
    """Main entry point for all SELECT queries.

    You shouldn't create instance of this class by yourself,
    instead you must use Table class.

    Example:
    ---------------------------
    ```
    class Buns(BaseTable, table_name="buns"):
        name: VarCharField = VarCharField()


    select_statement: SelectStatement = Buns.select()
    ```
    """

    def __init__(
        self: typing.Self,
        from_table: type[FromTable],
        select_fields: typing.Iterable[BaseField[typing.Any]],
    ) -> None:
        self._from_table: typing.Final[type[FromTable]] = from_table
        self._select_fields: typing.Iterable[
            BaseField[typing.Any],
        ] = select_fields
        self.final_select_fields: list[BaseField[typing.Any]] = []
        self.exist_prefixes: typing.Final[list[str]] = []

        self._filter_statement: FilterStatement = FilterStatement()
        self._limit_statement: LimitStatement = LimitStatement()
        self._offset_statement: OffsetStatement = OffsetStatement()
        self._order_by_statement: OrderByStatement = OrderByStatement()
        self._join_statement: JoinStatement = JoinStatement()
        self._field_aliases: FieldAliases = FieldAliases()

        self._as_objects: bool = False

    def __await__(
        self: typing.Self,
    ) -> typing.Generator[None, None, "SelectStatementResult[FromTable]"]:
        """SelectStatement can be awaited.

        Example:
        ---------------------------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()

        async def main() -> None:
            list_of_buns = await Buns.select()
            print(list_of_buns)
        ```
        """
        return self._run_query().__await__()

    async def execute(
        self: typing.Self,
        engine: BaseEngine[typing.Any, typing.Any],
    ) -> "SelectStatementResult[FromTable]":
        """Execute select statement.

        This is manual execution.
        You can pass specific engine and set if you want
        to retrieve table objects instead of list of dicts.

        ### Parameters
        :param engine: subclass of BaseEngine.

        ### Returns
        :returns: list of dicts or list of table instances.
        """
        from qaspen.statements.statement_result.select_result import (
            SelectStatementResult,
        )

        raw_query_result: list[
            tuple[typing.Any, ...],
        ] = await engine.run_query(
            querystring=self.querystring(),
        )

        query_result: SelectStatementResult[FromTable] = SelectStatementResult(
            from_table=self._from_table,
            query_result=raw_query_result,
            aliases=self._field_aliases,
        )

        return query_result

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
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
        self._filter_statement.where(*where_arguments)
        return self

    def limit(
        self: typing.Self,
        limit: int,
    ) -> typing.Self:
        """Set limit to the query.

        If you use this method more than 1 time, previous result
        will be overridden.

        Example:
        ------
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
        self: typing.Self,
        offset: int,
    ) -> typing.Self:
        """Set offset to the query.

        If you use this method more than 1 time, previous result
        will be overridden.

        Example:
        ------
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
        self: typing.Self,
        limit: int,
        offset: int,
    ) -> typing.Self:
        """Set offset and limit at the same time.

        See `limit()` and `offset()` methods description.
        """
        self._limit_statement.limit(limit_number=limit)
        self._offset_statement.offset(offset_number=offset)
        return self

    def order_by(
        self: typing.Self,
        field: BaseField[typing.Any] | None = None,
        ascending: bool = True,
        nulls_first: bool = True,
        order_bys: typing.Iterable[OrderBy] | None = None,
    ) -> typing.Self:
        """Set ORDER BY.

        You can specify `field`, `ascending` and `nulls_first`
        parameters and/or pass `order_bys` with OrderBy instances.

        ### Parameters
        :param field: subclass of BaseField.
        :param ascending: if True then `ASC` else `DESC`.
        :param nulls_first: if True then `NULLS FIRST` else `NULLS LAST`.
        :param order_bys: list of instances of order by.

        Example:
        -----
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
                order_by_statements=order_bys,
            )
        return self

    def union(
        self: typing.Self,
        union_with: "SelectStatement[FromTable]",
        union_all: bool = False,
    ) -> "UnionStatement":
        """Creates union statement.

        Combines two `SelectStatement`'s and creates
        `UnionStatement`.

        ### Parameters
        :param union_with: `SelectStatement` for union.
        :param union_all: if True than `UNION ALL` else `UNION`.

        ### Returns
        :returns: `UnionStatement`.

        Example:
        ------
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

        return UnionStatement(
            left_expression=self,
            right_expression=union_with,
            union_all=union_all,
        )

    def intersect(
        self: typing.Self,
        intersect_with: "SelectStatement[FromTable]",
    ) -> "IntersectStatement":
        """Create intersect statement.

        Combines two `SelectStatement`'s and creates
        `IntersectStatement`

        ### Parameters
        :param intersect_with: `SelectStatement` for intersect.

        ### Returns
        :returns: `IntersectStatement`.

        Example:
        ------
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

    def exists(self: typing.Self) -> "ExistsStatement":
        """Create exists statement.

        Create exists statement that return True or False.

        ### Returns
        :returns: `ExistsStatement`.

        Example:
        ------
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
        self: typing.Self,
        fields: list[Field[typing.Any]],
        based_on: CombinableExpression,
    ) -> typing.Self:
        """Add `JOIN` to the SelectStatement.

        You can specify what fields you want to get from
        the joined table.
        And set `ON` condition, it can be a combination.

        :param based_on: Field's comparisons.
        :param fields: fields to retrieve.

        Example:
        ------
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
            .select()
            .join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )

        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.JOIN,
        )

    def inner_join(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        """Add `INNER JOIN` to the SelectStatement.

        You can specify what fields you want to get from
        the joined table.
        And set `ON` condition, it can be a combination.

        :param based_on: Field's comparisons.
        :param fields: fields to retrieve.

        Example:
        ------
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
            .select()
            .inner_join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )
        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.INNERJOIN,
        )

    def left_join(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        """Add `LEFT JOIN` to the SelectStatement.

        You can specify what fields you want to get from
        the joined table.
        And set `ON` condition, it can be a combination.

        :param based_on: Field's comparisons.
        :param fields: fields to retrieve.

        Example:
        ------
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
            .select()
            .left_join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )

        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.LEFTJOIN,
        )

    def right_join(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        """Add `RIGHT JOIN` to the SelectStatement.

        You can specify what fields you want to get from
        the joined table.
        And set `ON` condition, it can be a combination.

        :param based_on: Field's comparisons.
        :param fields: fields to retrieve.

        Example:
        ------
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
            .select()
            .right_join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )

        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.RIGHTJOIN,
        )

    def full_outer_join(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> typing.Self:
        """Add `FULL OUTER JOIN` to the SelectStatement.

        You can specify what fields you want to get from
        the joined table.
        And set `ON` condition, it can be a combination.

        :param based_on: Field's comparisons.
        :param fields: fields to retrieve.

        Example:
        ------
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
            .select()
            .full_outer_join(
                fields=[
                    Cookies.filling,
                    Cookies.topping,
                ],
                based_on=Buns.name == Cookies.bun_name,
            )

        )
        ```

        :returns: `SelectStatement`
        """
        return self._join_on(
            based_on=based_on,
            fields=fields,
            join_type=JoinType.FULLOUTERJOIN,
        )

    def join_and_return(
        self: typing.Self,
        fields: list[Field[typing.Any]],
        based_on: CombinableExpression,
    ) -> Join:
        """Add `JOIN` to the SelectStatement and return instance of the `Join`.

        You can use it if you need to add more JOIN's based on
        previous joins.

        Example:
        ------
        ```
        class Computer(BaseTable, table_name="computer"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Processor(BaseTable, table_name="processor"):
            computer_name: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        class Architecture(BaseTable, table_name="Architecture")
            title: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        statement = Computer.select()

        processor_join = statement.join_and_return(
            fields=[Processor.processor_name],
            based_on=Computer.name == Processor.computer_name,
        )

        statement.join(
            fields=[Architecture.title],
            # VERY IMPORTANT! There is used `processor_join`.
            based_on=(
                processor_join.processor_name
                == Architecture.processor_name
            ),
        )
        ```
        """
        self.join(
            based_on=based_on,
            fields=fields,
        )
        return self._join_statement.join_expressions[-1]

    def inner_join_and_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> InnerJoin:
        """Add `INNER JOIN` to the Statement and return instance of the `Join`.

        You can use it if you need to add more JOIN's based on
        previous join/joins.

        Example:
        ------
        ```
        class Computer(BaseTable, table_name="computer"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Processor(BaseTable, table_name="processor"):
            computer_name: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        class Architecture(BaseTable, table_name="Architecture")
            title: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        statement = Computer.select()

        processor_join = statement.inner_join_and_return(
            fields=[Processor.processor_name],
            based_on=Computer.name == Processor.computer_name,
        )

        statement.join(
            fields=[Architecture.title],
            # VERY IMPORTANT! There is used `processor_join`.
            based_on=(
                processor_join.processor_name
                == Architecture.processor_name
            ),
        )
        ```
        """
        self.inner_join(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            InnerJoin,
            self._join_statement.join_expressions[-1],
        )

    def left_join_and_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> LeftOuterJoin:
        """Add `LEFT JOIN` to the Statement and return instance of the `Join`.

        You can use it if you need to add more JOIN's based on
        previous join/joins.

        Example:
        ------
        ```
        class Computer(BaseTable, table_name="computer"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Processor(BaseTable, table_name="processor"):
            computer_name: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        class Architecture(BaseTable, table_name="Architecture")
            title: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        statement = Computer.select()

        processor_join = statement.left_join_and_return(
            fields=[Processor.processor_name],
            based_on=Computer.name == Processor.computer_name,
        )

        statement.join(
            fields=[Architecture.title],
            # VERY IMPORTANT! There is used `processor_join`.
            based_on=(
                processor_join.processor_name
                == Architecture.processor_name
            ),
        )
        ```
        """
        self.left_join(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            LeftOuterJoin,
            self._join_statement.join_expressions[-1],
        )

    def right_join_and_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> RightOuterJoin:
        """Add `RIGHT JOIN` to the Statement and return instance of the `Join`.

        You can use it if you need to add more JOIN's based on
        previous join/joins.

        Example:
        ------
        ```
        class Computer(BaseTable, table_name="computer"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Processor(BaseTable, table_name="processor"):
            computer_name: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        class Architecture(BaseTable, table_name="Architecture")
            title: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        statement = Computer.select()

        processor_join = statement.right_join_and_return(
            fields=[Processor.processor_name],
            based_on=Computer.name == Processor.computer_name,
        )

        statement.join(
            fields=[Architecture.title],
            # VERY IMPORTANT! There is used `processor_join`.
            based_on=(
                processor_join.processor_name
                == Architecture.processor_name
            ),
        )
        ```
        """
        self.right_join(
            based_on=based_on,
            fields=fields,
        )
        return typing.cast(
            RightOuterJoin,
            self._join_statement.join_expressions[-1],
        )

    def full_outer_join_and_return(
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
    ) -> FullOuterJoin:
        """Add `FULL OUTER JOIN` to the Statement and return `Join` instance.

        You can use it if you need to add more JOIN's based on
        previous join/joins.

        Example:
        ------
        ```
        class Computer(BaseTable, table_name="computer"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Processor(BaseTable, table_name="processor"):
            computer_name: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        class Architecture(BaseTable, table_name="Architecture")
            title: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        statement = Computer.select()

        processor_join = statement.full_outer_join_and_return(
            fields=[Processor.processor_name],
            based_on=Computer.name == Processor.computer_name,
        )

        statement.join(
            fields=[Architecture.title],
            # VERY IMPORTANT! There is used `processor_join`.
            based_on=(
                processor_join.processor_name
                == Architecture.processor_name
            ),
        )
        ```
        """
        self.full_outer_join(
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
        """Add one ore more joins to the SelectStatement.

        You can create JOINs instances by yourself and pass here.

        Example:
        -------
        ```
        class Computer(BaseTable, table_name="computer"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        class Processor(BaseTable, table_name="processor"):
            computer_name: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        class Architecture(BaseTable, table_name="Architecture")
            title: VarCharField = VarCharField()
            processor_name: VarCharField = VarCharField()


        processor_join = Join(
            fields=[Processor.processor_name],
            from_table=Computer,
            join_table=Processor,
            on=Computer.name == Processor.computer_name,
            # You must control join aliases.
            join_alias="processor_join",
        )

        architecture_join = Join(
            fields=[Architecture.title],
            from_table=Computer,
            join_table=Architecture,
            on=processor_join.processor_name == Architecture.processor_name,
            join_alias="architecture_join",
        )

        statement = Computer.select().add_join(
            processor_join,
            architecture_join,
        )
        ```
        """
        self._join_statement.add_join(
            *join,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
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
        self: typing.Self,
        based_on: CombinableExpression,
        fields: list[Field[typing.Any]],
        join_type: JoinType,
    ) -> typing.Self:
        if not fields:
            raise OnJoinFieldsError(
                "You must specify fields to get from JOIN",
            )

        join_table: type["BaseTable"] = fields[0]._field_data.from_table

        self._join_statement.join(
            fields=fields,
            join_table=join_table,
            from_table=self._from_table,
            on=based_on,
            join_type=join_type,
        )
        return self

    async def _run_query(
        self: typing.Self,
    ) -> "SelectStatementResult[FromTable]":
        if not self._from_table._table_meta.database_engine:
            raise AttributeError(
                "There is no database engine.",
            )
        return await self.execute(
            engine=self._from_table._table_meta.database_engine,
        )

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

    def _select_from(self: typing.Self) -> QueryString:
        to_select_fields: str = ", ".join(
            [field.field_name for field in self._prepare_select_fields()],
        )
        if self._from_table._table_meta.alias:
            return QueryString(
                to_select_fields or "1",
                self._from_table._table_meta.table_name,
                self._from_table._table_meta.alias,
                sql_template="SELECT {} FROM {} as {}",
            )

        return QueryString(
            to_select_fields or "1",
            self._from_table._table_meta.table_name,
            sql_template="SELECT {} FROM {}",
        )
