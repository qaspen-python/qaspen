import abc
import dataclasses
import typing

from qaspen.fields.operators import ANDOperator, BaseOperator, OROperator
from qaspen.querystring.querystring import QueryString


class CombinableExpression(abc.ABC):
    @abc.abstractmethod
    def querystring(self: typing.Self) -> QueryString:
        ...

    def __and__(
        self: typing.Self,
        expression: "CombinableExpression",
    ) -> "ANDExpression":
        return ANDExpression(
            left_expression=self,
            right_expression=expression,
        )

    def __or__(
        self: typing.Self,
        expression: "CombinableExpression",
    ) -> "ORExpression":
        return ORExpression(
            left_expression=self,
            right_expression=expression,
        )


@dataclasses.dataclass
class ExpressionsCombination(CombinableExpression):
    left_expression: "CombinableExpression"
    right_expression: "CombinableExpression"
    operator: type[BaseOperator] = BaseOperator

    def querystring(self: typing.Self) -> QueryString:
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + self.operator.operation_template + " {}",
        )


@dataclasses.dataclass
class ANDExpression(ExpressionsCombination):
    operator: type[ANDOperator] = ANDOperator


@dataclasses.dataclass
class ORExpression(ExpressionsCombination):
    operator: type[OROperator] = OROperator
