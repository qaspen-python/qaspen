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

    def to_sql_statement(self: typing.Self) -> str:
        return (
            f"{self.left_expression.to_sql_statement()}"
            f" {self.operator.operation_template} "
            f"{self.right_expression.to_sql_statement()}"
        )


@dataclasses.dataclass
class ANDExpression(ExpressionsCombination):
    operator: type[ANDOperator] = ANDOperator


@dataclasses.dataclass
class ORExpression(ExpressionsCombination):
    operator: type[OROperator] = OROperator


class EmptyValue:
    pass


EMPTY_VALUE = EmptyValue()


class Where(CombinableExpression):
    def __init__(
        self: typing.Self,
        field: BaseField[typing.Any],
        operator: type[BaseOperator],
        comparison_value: EmptyValue | typing.Any = EMPTY_VALUE,
        comparison_values: EmptyValue | typing.Iterable[
            typing.Any,
        ] = EMPTY_VALUE,
    ) -> None:
        self.field: BaseField[typing.Any] = field
        self.operator: type[BaseOperator] = operator

        self.comparison_value: EmptyValue | typing.Any = comparison_value
        self.comparison_values: EmptyValue | typing.Iterable[
            typing.Any,
        ] = comparison_values

    def to_sql_statement(self: typing.Self) -> str:
        if self.comparison_value is not EMPTY_VALUE:
            compare_value: str = f"'{self.comparison_value}'"
        elif self.comparison_values is not EMPTY_VALUE:
            compare_value = ", ".join(
                [
                    f"'{comparison_value}'"
                    for comparison_value
                    in self.comparison_values  # type: ignore[union-attr]
                ]
            )

        where_clause: str = (
            self
            .operator
            .operation_template
            .format(
                field_name=self.field.field_name,
                compare_value=compare_value,
            )
        )

        return where_clause


class WhereBetween(CombinableExpression):
    def __init__(
        self: typing.Self,
        field: BaseField[typing.Any],
        operator: type[BaseOperator],
        left_comparison_value: typing.Any,
        right_comparison_value: typing.Any,
    ) -> None:
        self.field: BaseField[typing.Any] = field
        self.operator: type[BaseOperator] = operator

        self.left_comparison_value: typing.Any = left_comparison_value
        self.right_comparison_value: typing.Any = right_comparison_value

    def to_sql_statement(self: typing.Self) -> str:
        where_clause: str = (
            self
            .operator
            .operation_template
            .format(
                field_name=self.field.field_name,
                left_comparison_value=self.left_comparison_value,
                right_comparison_value=self.right_comparison_value,
            )
        )
        return where_clause
