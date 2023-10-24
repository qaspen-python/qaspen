from typing import Any, Final, List, Tuple

from typing_extensions import Self

from qaspen.base.sql_base import SQLSelectable
from qaspen.engine.base import BaseEngine
from qaspen.exceptions import QueryResultLookupError
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.statement import BaseStatement


class ExistsStatement(
    BaseStatement,
    CombinableExpression,
    SQLSelectable,
    Executable[bool],
):
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
        engine: BaseEngine[Any, Any],
    ) -> bool:
        """Execute Exists statement.

        You can pass custom engine in this method.

        :param engine: subclass of BaseEngine.
        """
        raw_query_result: List[Tuple[bool, ...],] = await engine.run_query(
            querystring=self.querystring_for_select(),
        )

        try:
            return raw_query_result[0][0]
        except LookupError as exc:
            raise QueryResultLookupError(
                "Cannot get result for ExistsStatement. "
                "Please check your statement.",
            ) from exc

    async def _run_query(
        self: Self,
    ) -> bool:
        if not self._select_statement._from_table._table_meta.database_engine:
            raise AttributeError(
                "There is no database engine.",
            )
        return await self.execute(
            engine=(
                self._select_statement._from_table._table_meta.database_engine
            ),
        )
