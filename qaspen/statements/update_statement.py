from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Final, Generic, List, Optional

from qaspen.exceptions import ColumnDeclarationError
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.filter_statement import (
    FilterStatement,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction
    from qaspen.columns.base import Column
    from qaspen.statements.combinable_statements.combinations import (
        CombinableExpression,
    )


class UpdateStatement(
    BaseStatement,
    Executable[Optional[List[Dict[str, Any]]]],
    Generic[FromTable],
):
    """Statement for UPDATE queries."""

    def __init__(
        self: Self,
        from_table: type[FromTable],
        for_update_map: dict[Column[Any], Any],
    ) -> None:
        self._from_table: Final = from_table
        self._for_update_map: Final = for_update_map

        self._filter_statement = FilterStatement(
            filter_operator="WHERE",
        )
        self._is_where_used: bool = False
        self._force: bool = False
        self._returning: tuple[Column[Any], ...] | None = None

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> list[dict[str, Any]] | None:
        """Execute update statement.

        This is manual execution.
        You can pass specific engine.

        ### Parameters
        - `engine`: subclass of BaseEngine.

        ### Returns
        `SelectStatementResult`
        """
        querystring, qs_parameters = self.querystring().build()
        raw_query_result: list[dict[str, Any]] | None = await engine.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=bool(self._returning),
        )

        return raw_query_result

    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> list[dict[str, Any]] | None:
        """Execute update statement inside a transaction context.

        This is manual execution.
        You can pass specific transaction.
        IMPORTANT: To commit the changes, with this way of execution,
        it's necessary to manually call `await transaction.commit()`.

        ### Parameters:
        - `transaction`: running transaction.
        database response or not.

        ### Returns
        `InsertStatement`
        """
        querystring, qs_parameters = self.querystring().build()
        raw_query_result: (
            list[dict[str, Any]] | None
        ) = await transaction.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=bool(self._returning),
        )

        return raw_query_result

    def where(
        self: Self,
        *where_arguments: CombinableExpression,
    ) -> Self:
        """Add where clause to the statement.

        It's possible to use this method as many times as you want.
        If you use `where` more than one time, clauses will be connected
        with `AND` operator.

        Columns have different methods for the comparison.
        Also, you can pass the combination of the `where` clauses.

        Below you will see easy and advanced examples.

        One Where Example:
        ------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()
            description: VarCharColumn = VarCharColumn()

        statement = Buns.update(
            {Buns.description: "Wow!"},
        ).where(
            Buns.name == "Tasty",
        )
        ```
        """
        self._is_where_used = True
        self._filter_statement.add_filter(*where_arguments)
        return self

    def querystring(self: Self) -> QueryString:
        """Build final querystring for UpdateStatement.

        Firstly it checks are there where clause or force flag,
        if not than raise an error.

        After it builds querystring, adds where clause
        and returning if necessary.

        ### Returns:
        `QueryString`.
        """
        if not self._is_where_used and not self._force:
            no_where_clause_error = (
                "You can't make UPDATE queries without WHERE clause. "
                "You can allow it with `.force()` method.",
            )
            raise ColumnDeclarationError(no_where_clause_error)
        querystring = self._main_query()

        if self._is_where_used:
            querystring += self._filter_statement.querystring()

        if self._returning:
            querystring += self._returning_query()

        return querystring

    def returning(
        self: Self,
        *returning_column: Column[Any],
    ) -> Self:
        """Add `RETURNING` to the query.

        ### Parameters:
        - `returning_column`: column to return

        ### Returns:
        `self`.
        """
        self._returning = returning_column
        return self

    def force(self: Self) -> Self:
        """Set force flag to True.

        It allows make requests to the database
        without WHERE clause.
        """
        self._force = True
        return self

    def deforce(self: Self) -> Self:
        """Set force flag to False.

        It disallows make requests to the database
        without WHERE clause.
        """
        self._force = False
        return self

    def _main_query(self: Self) -> QueryString:
        """Build main query.

        `UPDATE <> SET <>`.
        """
        updates: list[QueryString] = [
            QueryString(
                column_to_update._original_column_name,
                template_parameters=[update_value],
                sql_template=(
                    f"{QueryString.arg_ph()} = {QueryString.param_ph()}"
                ),
            )
            for column_to_update, update_value in self._for_update_map.items()
        ]
        updates_template = ", ".join(
            ["{}"] * len(self._for_update_map),
        )

        update_qs = QueryString(
            *updates,
            sql_template=updates_template,
        )

        return QueryString(
            self._from_table.table_name(),
            update_qs,
            sql_template="UPDATE {} SET {}",
        )

    def _returning_query(self: Self) -> QueryString:
        if not self._returning:  # pragma: no cover
            return QueryString.empty()

        returning_template: Final = ", ".join(
            ["{}"] * len(self._returning),
        )

        return QueryString(
            *self._returning,
            sql_template="RETURNING " + returning_template,
        )
