from __future__ import annotations

import abc
import dataclasses
from typing import TYPE_CHECKING

from qaspen.columns.operators import (
    ANDOperator,
    BaseOperator,
    NotOperator,
    OROperator,
)
from qaspen.querystring.querystring import QueryString

if TYPE_CHECKING:
    from typing_extensions import Self


class CombinableExpression(abc.ABC):
    """Base class for all classes that can be combined.

    It provides base realization for `add`, `or` and `not`.

    Usually used for `Filter` classes, so you can combine
    filter limitless.
    """

    @abc.abstractmethod
    def querystring(self: Self) -> QueryString:
        """Build new querystring for this expression."""
        ...  # pragma: no cover

    def __and__(
        self: Self,
        expression: CombinableExpression,
    ) -> ANDExpression:
        """Add new expression to exist one.

        It creates `ANDExpression`, that means that now
        two expression combined with `AND` SQL clause.

        ### Parameters:
        - `expression`: other combinable expression.

        ### Returns:
        `ANDExpression`
        """
        return ANDExpression(
            left_expression=self,
            right_expression=expression,
        )

    def __or__(
        self: Self,
        expression: CombinableExpression,
    ) -> ORExpression:
        """Combine two expressions into new one.

        It creates `ORExpression`, that means that now
        two expression combined with `OR` SQL clause.

        ### Parameters:
        - `expression`: other combinable expression.

        ### Returns:
        `ORExpression`
        """
        return ORExpression(
            left_expression=self,
            right_expression=expression,
        )

    def __invert__(self: Self) -> NotExpression:
        """Create invert expression.

        It creates `NotExpression`, that means that now
        expression will reverse the condition.

        ### Parameters:
        - `expression`: other combinable expression.

        ### Returns:
        `NotExpression`
        """
        return NotExpression(
            left_expression=self,
            right_expression=None,  # type: ignore[arg-type]
        )


@dataclasses.dataclass
class ExpressionsCombination(CombinableExpression):
    """Combination of CombinableExpressions.

    This class contains unlimited number of
    `CombinableExpression`, can aggregate them,
    and create one QueryString.
    """

    left_expression: CombinableExpression
    right_expression: CombinableExpression
    operator: type[BaseOperator] = BaseOperator

    def querystring(self: Self) -> QueryString:
        """Build new single `QueryString`.

        We recursively call `querystring` method
        for all `CombinableExpression` and create single
        QueryString.

        ### Returns:
        New `QueryString`
        """
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template=(
                f"{QueryString.arg_ph()} "
                + self.operator.operation_template
                + f" {QueryString.arg_ph()}"
            ),
        )


@dataclasses.dataclass
class ANDExpression(ExpressionsCombination):
    """Expression for `AND` PostgreSQL clause."""

    operator: type[ANDOperator] = ANDOperator


@dataclasses.dataclass
class ORExpression(ExpressionsCombination):
    """Expression for `OR` PostgreSQL clause."""

    operator: type[OROperator] = OROperator


@dataclasses.dataclass
class NotExpression(ExpressionsCombination):
    """Expression for `NOT` PostgreSQL clause."""

    operator: type[NotOperator] = NotOperator

    def querystring(self: Self) -> QueryString:
        """Build QueryString."""
        return QueryString(
            template_parameters=[self.left_expression.querystring()],
            sql_template=self.operator.operation_template,
        )
