import abc
import dataclasses
import typing

from qaspen.fields.base_field import Field
from qaspen.fields.operators import ANDOperator, BaseOperator, OROperator


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


@dataclasses.dataclass(slots=True)
class Where(CombinableExpression):
    field: Field[typing.Any]
    compare_with_value: typing.Any
    operator: type[BaseOperator]

    def to_sql_statement(self: typing.Self) -> str:
        where_clause: str = ""
        where_clause += (
            self
            .operator
            .operation_template
            .format(
                field_name=self.field.field_name,
                compare_value=f"'{self.compare_with_value}'",
            )
        )

        return where_clause
