import typing
from qaspen.fields.base_field import BaseField


from qaspen.fields.operators import BaseOperator
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression
)


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


class WhereExclusive(CombinableExpression):

    def __init__(
        self: typing.Self,
        comparison: CombinableExpression,
    ) -> None:
        self.comparisons: CombinableExpression = comparison

    def to_sql_statement(self: typing.Self) -> str:
        return f"({self.comparisons.to_sql_statement()})"


class WhereStatement:
    where_expressions: list[CombinableExpression] = []

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
        self.where_expressions.extend(
            where_arguments,
        )
        return self

    def _build_query(self: typing.Self) -> str:
        filter_params: str = "AND".join(
            [
                f" {where_statement.to_sql_statement()} "
                for where_statement
                in self.where_expressions
            ]
        )

        return "WHERE" + filter_params + " "
