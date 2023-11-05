from __future__ import annotations

from typing import TYPE_CHECKING, Final

from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

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


class IntersectStatement(BaseStatement):
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
