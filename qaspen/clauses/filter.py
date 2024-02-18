from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Iterable

from qaspen.qaspen_types import EMPTY_VALUE, EmptyValue
from qaspen.querystring.querystring import FilterQueryString, QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLComparison
    from qaspen.columns.operators import BaseOperator


class Filter(CombinableExpression):
    """Class that represents any Filter in PostgreSQL.

    It is used in the places where you use comparisons like
    `eq` and etc.
    This is usually used for `WHERE` and `ON` clauses.
    """

    def __init__(
        self: Self,
        left_operand: SQLComparison[Any],
        operator: type[BaseOperator],
        comparison_value: Any = EMPTY_VALUE,
        comparison_values: EmptyValue | Iterable[Any] = EMPTY_VALUE,
    ) -> None:
        self.left_operand: Final = left_operand
        self.operator: Final = operator

        self.comparison_value: Final = comparison_value
        self.comparison_values: Final = comparison_values

    def querystring(self: Self) -> FilterQueryString:
        """Build new `FilterQueryString`."""
        compare_value: Any = None
        if self.comparison_value is not EMPTY_VALUE:
            compare_value = self.comparison_value

        elif self.comparison_values is not EMPTY_VALUE:
            compare_value = QueryString(
                template_parameters=[self.comparison_values],
                sql_template=f"{QueryString.param_ph()}",
            )

        return FilterQueryString(
            self.left_operand.querystring(),
            template_parameters=([compare_value] if compare_value else []),
            sql_template=self.operator.operation_template,
        )


class FilterBetween(CombinableExpression):
    """Class that represents BETWEEN filter in PostgreSQL.

    It is used in the places where you use comparisons like
    `eq` and etc.
    This is usually used for `WHERE` and `ON` clauses.
    """

    def __init__(
        self: Self,
        column: SQLComparison[Any],
        operator: type[BaseOperator],
        left_comparison_value: Any,
        right_comparison_value: Any,
    ) -> None:
        self.column: Final = column
        self.operator: Final = operator

        self.left_comparison_value: Final = left_comparison_value
        self.right_comparison_value: Final = right_comparison_value

    def querystring(self: Self) -> FilterQueryString:
        """Build new `FilterQueryString`."""
        from qaspen.columns.base import BaseColumn

        left_value: str = (
            self.left_comparison_value.column_name
            if isinstance(self.left_comparison_value, BaseColumn)
            else self.left_comparison_value
        )

        right_value: str = (
            self.right_comparison_value.column_name
            if isinstance(self.right_comparison_value, BaseColumn)
            else self.right_comparison_value
        )

        return FilterQueryString(
            self.column.querystring(),
            template_parameters=[left_value, right_value],
            sql_template=self.operator.operation_template,
        )


class FilterExclusive(CombinableExpression):
    """Special class that can isolate Filters in brackets."""

    def __init__(
        self: Self,
        comparison: CombinableExpression,
    ) -> None:
        """Initialize FilterExclusive.

        This class isolates expressions, so you can build
        more complex queries.
        """
        self.comparison: Final = comparison

    def querystring(self: Self) -> FilterQueryString:
        """Build new `FilterQueryString`."""
        comparison_qs: Final = self.comparison.querystring()
        return FilterQueryString(
            *comparison_qs.template_arguments,
            template_parameters=comparison_qs.template_parameters,
            sql_template=("(" + comparison_qs.sql_template + ")"),
        )
