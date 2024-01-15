from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Final, Generic, List, Optional

from qaspen.exceptions import FieldDeclarationError
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
    from qaspen.fields.base import Field
    from qaspen.statements.combinable_statements.combinations import (
        CombinableExpression,
    )


class DeleteStatement(
    BaseStatement,
    Executable[Optional[List[Dict[str, Any]]]],
    Generic[FromTable],
):
    """Statement for DELETE queries."""

    def __init__(self: Self, from_table: type[FromTable]) -> None:
        self._from_table: Final = from_table

        self._filter_statement = FilterStatement(
            filter_operator="WHERE",
        )
        self._is_where_used: bool = False
        self._force: bool = False
        self._returning: tuple[Field[Any], ...] | None = None

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> list[dict[str, Any]] | None:
        """Execute DELETE statement.

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
        """Execute DELETE statement inside a transaction context.

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
        raw_query_result: list[
            dict[str, Any]
        ] | None = await transaction.execute(
            querystring=querystring,
            querystring_parameters=qs_parameters,
            fetch_results=bool(self._returning),
        )

        return raw_query_result

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
                "You can't make DELETE queries without WHERE clause. "
                "You can allow it with `.force()` method.",
            )
            raise FieldDeclarationError(no_where_clause_error)

        querystring = QueryString(
            self._from_table.table_name(),
            sql_template=f"DELETE FROM {QueryString.arg_ph()}",
        )

        if self._is_where_used:
            querystring += self._filter_statement.querystring()

        if self._returning:
            querystring += self._returning_query()

        return querystring

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

        statement = Buns.delete().where(
            Buns.name == "Tasty",
        )
        ```
        """
        self._is_where_used = True
        self._filter_statement.add_filter(*where_arguments)
        return self

    def returning(
        self: Self,
        *returning_field: Field[Any],
    ) -> Self:
        """Add `RETURNING` to the query.

        ### Parameters:
        - `returning_field`: field to return

        ### Returns:
        `self`.
        """
        self._returning = returning_field
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
