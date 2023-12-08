from __future__ import annotations

import dataclasses
import functools
import operator
from typing import TYPE_CHECKING, Any, Final, Iterable

from qaspen.qaspen_types import EMPTY_VALUE, EmptyValue
from qaspen.querystring.querystring import FilterQueryString, QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field
    from qaspen.fields.operators import BaseOperator


class Filter(CombinableExpression):
    """Class that represents any Filter in PostgreSQL.

    It is used in the places where you use comparisons like
    `eq` and etc.
    This is usually used for `WHERE` and `ON` clauses.
    """

    def __init__(
        self: Self,
        field: Field[Any],
        operator: type[BaseOperator],
        comparison_value: Any = EMPTY_VALUE,
        comparison_values: EmptyValue | Iterable[Any] = EMPTY_VALUE,
    ) -> None:
        self.field: Final = field
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
            self.field.field_name,
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
        field: Field[Any],
        operator: type[BaseOperator],
        left_comparison_value: Any,
        right_comparison_value: Any,
    ) -> None:
        self.field: Final = field
        self.operator: Final = operator

        self.left_comparison_value: Final = left_comparison_value
        self.right_comparison_value: Final = right_comparison_value

    def querystring(self: Self) -> FilterQueryString:
        """Build new `FilterQueryString`."""
        from qaspen.fields.base import BaseField

        left_value: str = (
            self.left_comparison_value.field_name
            if isinstance(self.left_comparison_value, BaseField)
            else self.left_comparison_value
        )

        right_value: str = (
            self.right_comparison_value.field_name
            if isinstance(self.right_comparison_value, BaseField)
            else self.right_comparison_value
        )

        return FilterQueryString(
            self.field.field_name,
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


@dataclasses.dataclass
class FilterStatement(BaseStatement):
    """Filter statement for high-level statements.

    It is used in Select/Update/Insert/Delete Statements.
    """

    filter_expressions: list[CombinableExpression] = dataclasses.field(
        default_factory=list,
    )

    def add_filter(
        self: Self,
        *filter_argument: CombinableExpression,
    ) -> Self:
        """Add new CombinableExpression's.

        ### Parameters:
        - `filter_argument`: CombinableExpressions.

        ### Returns:
        `self`.
        """
        self.filter_expressions.extend(
            filter_argument,
        )
        return self

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`.

        Adds all statements to each other.

        If there is no `filter_expressions`, then
        return `EmptyQueryString`.

        ### Returns:
        `QueryString`
        """
        if not self.filter_expressions:
            return QueryString.empty()

        final_wheres = []
        for filter_expression in self.filter_expressions:
            filter_qs = filter_expression.querystring()
            final_wheres.append(
                FilterQueryString(
                    *filter_qs.template_arguments,
                    template_parameters=filter_qs.template_parameters,
                    sql_template=filter_qs.sql_template,
                ),
            )

        final_where: QueryString = functools.reduce(
            operator.add,
            final_wheres,
        )

        return QueryString(
            *final_where.template_arguments,
            sql_template=f"WHERE {final_where.sql_template}",
            template_parameters=final_where.template_parameters,
        )
