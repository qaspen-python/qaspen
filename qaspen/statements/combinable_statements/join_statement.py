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

    from qaspen.fields.base import Field
    from qaspen.qaspen_types import FieldType
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
        fields: Iterable[Field[Any]] | None,
        from_table: type[BaseTable],
        join_table: type[BaseTable],
        on: CombinableExpression,
        join_alias: str,
    ) -> None:
        self._from_table: Final[type[BaseTable]] = from_table
        self._join_table: Final[type[BaseTable]] = join_table
        self._based_on: CombinableExpression = on
        self._alias: str = join_alias

        self._fields: list[Field[Any]] | None = None
        if fields:
            self._fields = self._process_select_fields(
                fields=fields,
            )

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        return QueryString(
            self.join_type,
            self._join_table.schemed_original_table_name(),
            self._join_table._table_meta.alias or self._alias,
            self._based_on.querystring(),
            sql_template="{} {} AS {} ON {}",
        )

    def add_fields(
        self: Self,
        fields: list[Field[Any]],
    ) -> None:
        """Add new fields to the join.

        ### Parameters:
        - `fields`: fields to add.
        """
        processed_fields = self._process_select_fields(
            fields=fields,
        )
        if self._fields:
            self._fields.extend(processed_fields)
            return

        self._fields = processed_fields

    def _prefixed_field(
        self: Self,
        field: Field[FieldType],
    ) -> Field[FieldType]:
        return field._with_prefix(
            prefix=(
                field._field_data.from_table._table_meta.alias or self._alias
            ),
        )

    def _process_select_fields(
        self: Self,
        fields: Iterable[Field[Any]],
    ) -> list[Field[Any]]:
        fields_with_prefix: list[Field[Any]] = [
            self._prefixed_field(field=field) for field in fields
        ]
        return fields_with_prefix

    def _join_fields(self: Self) -> list[Field[Any]] | None:
        return self._fields


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
        fields: Iterable[Field[Any]] | None = None,
    ) -> None:
        """Create new join.

        ### Parameters:
        - `join_table`: Table for join.
        - `from_table`: main Table from the query.
        - `on`: `ON` condition (Filter class usually).
        - `join_type`: type of the JOIN.
        - `fields`: fields to select from `join_table`.
        """
        join_alias = (
            join_table._table_meta.alias or join_table.original_table_name()
        )
        self.join_expressions.append(
            join_type.value(
                join_alias=join_alias,
                fields=fields,
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

    def _retrieve_all_join_fields(
        self: Self,
    ) -> list[Field[Any]]:
        all_joins_fields: list[Field[Any]] = []
        for join_expression in self.join_expressions:
            if join_fields := join_expression._join_fields():
                all_joins_fields.extend(join_fields)
        return all_joins_fields
