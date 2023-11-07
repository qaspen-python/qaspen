from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Generator, Generic

from qaspen.base.sql_base import SQLSelectable
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement
from qaspen.statements.statement_result.select_result import (
    SelectStatementResult,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.statements.select_statement import SelectStatement


class SQLUnion(CombinableExpression):
    """Main class for PostgreSQL `UNION`."""

    def __init__(
        self: Self,
        left_expression: SelectStatement[FromTable] | SQLUnion,
        right_expression: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> None:
        self.left_expression: Final = left_expression
        self.right_expression: Final = right_expression
        self.union_all: bool = union_all

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
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
    Generic[FromTable],
):
    """Union statement.

    Create with two or more `SelectStatements`.

    `union_statement` main SQLUnion object.
    """

    union_statement: SQLUnion

    def __init__(
        self: Self,
        left_expression: SelectStatement[FromTable] | SQLUnion,
        right_expression: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> None:
        self.union_statement: SQLUnion = SQLUnion(
            left_expression=left_expression,
            right_expression=right_expression,
            union_all=union_all,
        )

    def __await__(
        self: Self,
    ) -> Generator[None, None, SelectStatementResult[FromTable]]:
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
        return self._run_query().__await__()

    def union(
        self: Self,
        select_statement: SelectStatement[FromTable],
        union_all: bool = False,
    ) -> Self:
        """Add new `SelectStatement` to the `SQLUnion`.

        ### Parameters:
        - `select_statement`: initialized `SelectStatement`.
        - `union_all`: use `UNION` or `UNION ALL`.

        ### Returns:
        `self`
        """
        self.union_statement = SQLUnion(
            left_expression=self.union_statement,
            right_expression=select_statement,
            union_all=union_all,
        )
        return self

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`."""
        return self.union_statement.querystring()

    def _start_select_statement(
        self: Self,
        statement: SelectStatement[FromTable] | SQLUnion,
    ) -> SelectStatement[FromTable]:
        """Retrieve first SelectStatement in the UnionStatement.

        ### Parameters
        :param statement: Union or SelectStatement.

        ### Returns
        :returns: SelectStatement.
        """
        if isinstance(statement, SQLUnion):
            return self._start_select_statement(
                statement=statement.left_expression,  # type: ignore[arg-type]
            )
        return statement

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any, Any],
    ) -> SelectStatementResult[FromTable]:
        """Execute SQL query and return result."""
        raw_query_result: list[tuple[Any, ...],] = await engine.execute(
            querystring=self.querystring(),
        )

        first_select_statement: Final = self._start_select_statement(
            statement=self.union_statement.left_expression,  # type: ignore[arg-type]
        )

        query_result: SelectStatementResult[FromTable] = SelectStatementResult(
            from_table=first_select_statement._from_table,
            query_result=raw_query_result,
            aliases=first_select_statement._field_aliases,
        )

        return query_result

    async def _run_query(
        self: Self,
    ) -> SelectStatementResult[FromTable]:
        first_select_statement: Final = self._start_select_statement(
            statement=self.union_statement.left_expression,  # type: ignore[arg-type]
        )

        if not first_select_statement._from_table._table_meta.database_engine:
            engine_err_msg: Final = "There is no database engine."
            raise AttributeError(engine_err_msg)
        return await self.execute(
            engine=first_select_statement._from_table._table_meta.database_engine,
        )
