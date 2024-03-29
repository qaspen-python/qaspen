from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Final, Generic, List

from qaspen.base.sql_base import SQLSelectable
from qaspen.qaspen_types import FromTable
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
    Executable[List[Dict[str, Any]]],
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
