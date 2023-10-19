import typing

from qaspen.base.sql_base import SQLSelectable
from qaspen.engine.base import BaseEngine
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.statement import BaseStatement
from qaspen.statements.statement_result.select_result import (
    SelectStatementResult,
)


class Union(CombinableExpression):
    def __init__(
        self: typing.Self,
        left_expression: "SelectStatement[FromTable] | Union",
        right_expression: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> None:
        self.left_expression: SelectStatement[
            FromTable
        ] | "Union" = left_expression
        self.right_expression: SelectStatement[FromTable] = right_expression
        self.union_all: bool = union_all

    def querystring(self: typing.Self) -> QueryString:
        union_operator: str = "UNION ALL" if self.union_all else "UNION"
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + union_operator + " {}",
        )


class UnionStatement(
    BaseStatement,
    SQLSelectable,
    Executable[SelectStatementResult[FromTable]],
    typing.Generic[FromTable],
):
    union_statement: Union

    def __init__(
        self: typing.Self,
        left_expression: SelectStatement[FromTable] | Union,
        right_expression: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> None:
        self.union_statement: Union = Union(
            left_expression=left_expression,
            right_expression=right_expression,
            union_all=union_all,
        )

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

    def union(
        self: typing.Self,
        select_statement: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> typing.Self:
        self.union_statement = Union(
            left_expression=self.union_statement,
            right_expression=select_statement,
            union_all=union_all,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
        return self.union_statement.querystring()

    def build_query(self: typing.Self) -> str:
        return str(self.querystring())

    def _start_select_statement(
        self: typing.Self,
        statement: SelectStatement[FromTable] | Union,
    ) -> SelectStatement[FromTable]:
        """Retrieve first SelectStatement in the UnionStatement.

        ### Parameters
        :param statement: Union or SelectStatement.

        ### Returns
        :returns: SelectStatement.
        """
        if isinstance(statement, Union):
            return self._start_select_statement(
                statement=statement.left_expression,  # type: ignore[arg-type]
            )
        return statement

    async def execute(
        self: typing.Self,
        engine: BaseEngine[typing.Any, typing.Any],
    ) -> SelectStatementResult[FromTable]:
        """Execute SQL query and return result."""
        raw_query_result: list[
            tuple[typing.Any, ...],
        ] = await engine.run_query(
            querystring=self.querystring(),
        )

        first_select_statement: typing.Final = self._start_select_statement(
            statement=self.union_statement.left_expression,  # type: ignore[arg-type]
        )

        query_result: SelectStatementResult[FromTable] = SelectStatementResult(
            from_table=first_select_statement._from_table,
            query_result=raw_query_result,
            aliases=first_select_statement._field_aliases,
        )

        return query_result

    async def _run_query(
        self: typing.Self,
    ) -> "SelectStatementResult[FromTable]":
        first_select_statement: typing.Final = self._start_select_statement(
            statement=self.union_statement.left_expression,  # type: ignore[arg-type]
        )

        if not first_select_statement._from_table._table_meta.database_engine:
            raise AttributeError(
                "There is no database engine.",
            )
        return await self.execute(
            engine=first_select_statement._from_table._table_meta.database_engine,
        )
