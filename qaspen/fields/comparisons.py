import abc
import dataclasses
import typing

from qaspen.fields.operators import ANDOperator, BaseOperator, OROperator
from qaspen.fields.base_field import BaseField


class CombinableExpression(abc.ABC):
    @abc.abstractmethod
    def to_sql_statement(self: typing.Self) -> str:
        ...

    def __and__(
        self: typing.Self,
        expression: "Where | ANDExpression | ORExpression",
    ) -> "ANDExpression":
        return ANDExpression(
            left_expression=self,  # type: ignore[arg-type]
            right_expression=expression,
        )

    def __or__(
        self: typing.Self,
        expression: "Where | ANDExpression | ORExpression",
    ) -> "ORExpression":
        return ORExpression(
            left_expression=self,  # type: ignore[arg-type]
            right_expression=expression,
        )


@dataclasses.dataclass
class ExpressionsCombination(CombinableExpression):
    left_expression: "Where | ANDExpression | ORExpression"
    right_expression: "Where | ANDExpression | ORExpression"
    operator: type[BaseOperator] = BaseOperator

    def to_sql_statement(self: typing.Self) -> str:
        return (
            f"{self.left_expression.to_sql_statement()} "
            f"{self.operator.operation_template} "
            f"{self.right_expression.to_sql_statement()}"
        )


@dataclasses.dataclass
class ANDExpression(ExpressionsCombination):
    operator: type[ANDOperator] = ANDOperator


@dataclasses.dataclass
class ORExpression(ExpressionsCombination):
    operator: type[OROperator] = OROperator


class Where(CombinableExpression):
    def __init__(
        self: typing.Self,
        field: BaseField[typing.Any],
        compare_with_value: typing.Any,
        operator: type[BaseOperator],
    ) -> None:
        self.field = field
        self.compare_with_value: typing.Any = compare_with_value
        self.operator: type[BaseOperator] = operator

    def to_sql_statement(self: typing.Self) -> str:
        where_clause: str = ""
        where_clause += (
            self
            .operator
            .operation_template
            .format(
                field_name=self.field._field_data.field_name,
                compare_value=f"'{self.compare_with_value}'",
            )
        )

        return where_clause
