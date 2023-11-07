from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from qaspen.exceptions import QueryResultLookupError
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.qaspen_types import FromTable
    from qaspen.statements.select_statement import SelectStatement


class ExistsStatement(
    BaseStatement,
    CombinableExpression,
    Executable[bool],
):
    """Statement for `Exists` Statement."""

    def __init__(
        self: Self,
        select_statement: SelectStatement[FromTable],
    ) -> None:
        self._select_statement: Final[
            SelectStatement[FromTable],
        ] = select_statement

    def __await__(self: Self) -> Any:
        """Make ExistsStatement awaitable."""
        return self._run_query().__await__()

    def querystring(self: Self) -> QueryString:
        """Return querystring for comparisons.

        Used in methods like `Field.contains(querystring=...)`.
        """
        return QueryString(
            self._select_statement.querystring(),
            sql_template="EXISTS ({})",
        )

    def querystring_for_select(self: Self) -> QueryString:
        """Create querystring for SELECT."""
        return QueryString(
            self._select_statement.querystring(),
            sql_template="SELECT EXISTS ({})",
        )

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any, Any],
    ) -> bool:
        """Execute Exists statement.

        You can pass custom engine in this method.

        :param engine: subclass of BaseEngine.
        """
        raw_query_result: list[tuple[bool, ...],] = await engine.execute(
            querystring=self.querystring_for_select(),
        )

        try:
            return raw_query_result[0][0]
        except LookupError as exc:
            lookup_err_msg: Final = (
                "Cannot get result for ExistsStatement. "
                "Please check your statement.",
            )
            raise QueryResultLookupError(
                lookup_err_msg,
            ) from exc

    async def _run_query(
        self: Self,
    ) -> bool:
        if not self._select_statement._from_table._table_meta.database_engine:
            no_engine_err_msg: Final = "There is no database engine."
            raise AttributeError(no_engine_err_msg)
        return await self.execute(
            engine=(
                self._select_statement._from_table._table_meta.database_engine
            ),
        )
