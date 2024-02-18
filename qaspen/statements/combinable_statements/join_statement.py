from __future__ import annotations

import dataclasses
import enum
import functools
import operator
from typing import TYPE_CHECKING, Any, Final, Iterable

from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.columns.base import Column
    from qaspen.qaspen_types import ColumnType
    from qaspen.table.base_table import BaseTable


class Join(CombinableExpression):
    """Main class for PostgreSQL JOINs.

    It can work alone as a class and it's possible
    to inherit from it to create new types of JOINs.

    `join_type` is used for creating QueryString,
    basically it is join type.
    """

    join_type: str = "JOIN"

    def __init__(
        self: Self,
        columns: Iterable[Column[Any]] | None,
        from_table: type[BaseTable],
        join_table: type[BaseTable],
        on: CombinableExpression,
        join_alias: str,
    ) -> None:
        self._from_table: Final[type[BaseTable]] = from_table
        self._join_table: Final[type[BaseTable]] = join_table
        self._based_on: CombinableExpression = on
        self._alias: str = join_alias

        self._columns: list[Column[Any]] | None = None
        if columns:
            self._columns = self._process_select_columns(
                columns=columns,
            )

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        return QueryString(
            self.join_type,
            self._join_table.schemed_original_table_name(),
            self._join_table._table_meta.alias or self._alias,
            self._based_on.querystring(),
            sql_template=(
                f"{QueryString.arg_ph()} {QueryString.arg_ph()} "
                f"AS {QueryString.arg_ph()} ON {QueryString.arg_ph()}"
            ),
        )

    def add_columns(
        self: Self,
        columns: list[Column[Any]],
    ) -> None:
        """Add new columns to the join.

        ### Parameters:
        - `columns`: columns to add.
        """
        processed_columns = self._process_select_columns(
            columns=columns,
        )
        if self._columns:
            self._columns.extend(processed_columns)
            return

        self._columns = processed_columns

    def _prefixed_column(
        self: Self,
        column: Column[ColumnType],
    ) -> Column[ColumnType]:
        return column._with_prefix(
            prefix=(
                column._column_data.from_table._table_meta.alias or self._alias
            ),
        )

    def _process_select_columns(
        self: Self,
        columns: Iterable[Column[Any]],
    ) -> list[Column[Any]]:
        columns_with_prefix: list[Column[Any]] = [
            self._prefixed_column(column=column) for column in columns
        ]
        return columns_with_prefix

    def _join_columns(self: Self) -> list[Column[Any]] | None:
        return self._columns


class InnerJoin(Join):
    """Class for `INNER JOIN` join type."""

    join_type: str = "INNER JOIN"


class LeftOuterJoin(Join):
    """Class for `LEFT JOIN` join type."""

    join_type: str = "LEFT JOIN"


class RightOuterJoin(Join):
    """Class for `RIGHT JOIN` join type."""

    join_type: str = "RIGHT JOIN"


class FullOuterJoin(Join):
    """Class for `FULL OUTER JOIN` join type."""

    join_type: str = "FULL OUTER JOIN"


class JoinType(enum.Enum):
    """ENUM for JoinType."""

    JOIN: type[Join] = Join
    INNERJOIN: type[InnerJoin] = InnerJoin
    LEFTJOIN: type[LeftOuterJoin] = LeftOuterJoin
    RIGHTJOIN: type[RightOuterJoin] = RightOuterJoin
    FULLOUTERJOIN: type[FullOuterJoin] = FullOuterJoin


@dataclasses.dataclass
class JoinStatement(BaseStatement):
    """Join statement for high-level statements.

    It is used in Select/Update/Insert/Delete Statements.

    `join_expressions` contains all created joins.
    """

    join_expressions: list[Join] = dataclasses.field(
        default_factory=list,
    )

    def join(
        self: Self,
        join_table: type[BaseTable],
        from_table: type[BaseTable],
        on: CombinableExpression,
        join_type: JoinType,
        columns: Iterable[Column[Any]] | None = None,
    ) -> None:
        """Create new join.

        ### Parameters:
        - `join_table`: Table for join.
        - `from_table`: main Table from the query.
        - `on`: `ON` condition (Filter class usually).
        - `join_type`: type of the JOIN.
        - `columns`: columns to select from `join_table`.
        """
        join_alias = (
            join_table._table_meta.alias or join_table.original_table_name()
        )
        self.join_expressions.append(
            join_type.value(
                join_alias=join_alias,
                columns=columns,
                join_table=join_table,
                from_table=from_table,
                on=on,
            ),
        )

    def add_join(
        self: Self,
        *join_expression: Join,
    ) -> None:
        """Add new join.

        ### Parameters:
        - `join_expression`: instance of Join (or its subclasses).
        """
        self.join_expressions.extend(join_expression)

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        if not self.join_expressions:
            return QueryString.empty()

        final_join: QueryString = functools.reduce(
            operator.add,
            [
                join_expression.querystring()
                for join_expression in self.join_expressions
            ],
        )
        return final_join

    def _retrieve_all_join_columns(
        self: Self,
    ) -> list[Column[Any]]:
        all_joins_columns: list[Column[Any]] = []
        for join_expression in self.join_expressions:
            if join_columns := join_expression._join_columns():
                all_joins_columns.extend(join_columns)
        return all_joins_columns
