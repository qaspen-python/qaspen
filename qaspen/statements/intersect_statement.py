from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Final, Generator, List

from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction
    from qaspen.qaspen_types import FromTable
    from qaspen.statements.select_statement import SelectStatement


class Intersect(CombinableExpression):
    """Class represents `Intersect` object.

    It can contain 1 or more `Intersect` objects.
    """

    def __init__(
        self: Self,
        left_expression: SelectStatement[FromTable] | Intersect,
        right_expression: SelectStatement[FromTable],
    ) -> None:
        self.left_expression: Final = left_expression
        self.right_expression: Final = right_expression

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + "INTERSECT" + " {}",
        )


class IntersectStatement(
    BaseStatement,
    Executable[List[Dict[str, Any]]],
):
    """Intersect statement to aggregate `SelectStatement`.

    `intersect_statement` Intersect .
    """

    intersect_statement: Intersect

    def __init__(
        self: Self,
        left_expression: SelectStatement[FromTable] | Intersect,
        right_expression: SelectStatement[FromTable],
    ) -> None:
        self.intersect_statement: Intersect = Intersect(
            left_expression=left_expression,
            right_expression=right_expression,
        )

    def __await__(
        self: Self,
    ) -> Generator[None, None, list[dict[str, Any]]]:
        """IntersectStatement can be awaited.

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
        first_select_statement: Final = self._start_select_statement(
            statement=self.intersect_statement.left_expression,
        )

        engine: BaseEngine[
            Any,
            Any,
            Any,
        ] | None = (
            first_select_statement._from_table._table_meta.database_engine
        )
        if not engine:
            engine_err_msg: Final = "There is no database engine."
            raise AttributeError(engine_err_msg)

        return self.execute(engine=engine).__await__()

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> list[dict[str, Any]]:
        """Execute SQL query and return result."""
        querystring, qs_parameters = self.querystring().build()
        raw_query_result: list[dict[str, Any]] = await engine.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=True,
        )
        return raw_query_result

    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> list[dict[str, Any]]:
        """Execute SQL query in a transaction and return result."""
        querystring, qs_parameters = self.querystring().build()
        raw_query_result: list[dict[str, Any]] = await transaction.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=True,
        )
        return raw_query_result

    def intersect(
        self: Self,
        select_statement: SelectStatement[FromTable],
    ) -> Self:
        """Create `Intersect` with `SelectStatement`.

        ### Parameters:
        - `select_statement`: initialized `SelectStatement`.

        ### Returns:
        `self`.
        """
        self.intersect_statement = Intersect(
            left_expression=self.intersect_statement,
            right_expression=select_statement,
        )
        return self

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        return self.intersect_statement.querystring()

    def _start_select_statement(
        self: Self,
        statement: SelectStatement[FromTable] | Intersect,
    ) -> SelectStatement[FromTable]:
        """Retrieve first SelectStatement in the IntersectStatement.

        ### Parameters
        :param statement: Intersect or SelectStatement.

        ### Returns
        :returns: SelectStatement.
        """
        if isinstance(statement, Intersect):
            return self._start_select_statement(
                statement=statement.left_expression,
            )
        return statement
