import abc
import dataclasses
from typing import Type

from typing_extensions import Self

from qaspen.fields.operators import ANDOperator, BaseOperator, OROperator
from qaspen.querystring.querystring import QueryString


class CombinableExpression(abc.ABC):
    @abc.abstractmethod
    def querystring(self: Self) -> QueryString:
        ...

    def __and__(
        self: Self,
        expression: "CombinableExpression",
    ) -> "ANDExpression":
        return ANDExpression(
            left_expression=self,
            right_expression=expression,
        )

    def __or__(
        self: Self,
        expression: "CombinableExpression",
    ) -> "ORExpression":
        return ORExpression(
            left_expression=self,
            right_expression=expression,
        )

    def __invert__(self: Self) -> "NotExpression":
        return NotExpression(
            left_expression=self,
            right_expression=None,  # type: ignore[arg-type]
        )


@dataclasses.dataclass
class ExpressionsCombination(CombinableExpression):
    left_expression: "CombinableExpression"
    right_expression: "CombinableExpression"
    operator: Type[BaseOperator] = BaseOperator

    def querystring(self: Self) -> QueryString:
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + self.operator.operation_template + " {}",
        )


@dataclasses.dataclass
class ANDExpression(ExpressionsCombination):
    operator: Type[ANDOperator] = ANDOperator


@dataclasses.dataclass
class ORExpression(ExpressionsCombination):
    operator: Type[OROperator] = OROperator


@dataclasses.dataclass
class NotExpression(ExpressionsCombination):
    def querystring(self: Self) -> QueryString:
        return QueryString(
            self.left_expression.querystring(),
            sql_template="NOT (" + "{}" + ")",
        )
