import dataclasses
import functools
import operator
import typing

from qaspen.base.sql_base import SQLSelectable
from qaspen.fields.base_field import BaseField
from qaspen.fields.operators import BaseOperator
from qaspen.querystring.querystring import FilterQueryString, QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement
from qaspen.utils.fields_utils import transform_value_to_sql


class EmptyValue:
    pass


EMPTY_VALUE = EmptyValue()


class Filter(CombinableExpression):
    def __init__(
        self: typing.Self,
        field: BaseField[typing.Any],
        operator: type[BaseOperator],
        comparison_value: (
            EmptyValue | BaseField[typing.Any] | typing.Any
        ) = EMPTY_VALUE,
        comparison_values: EmptyValue
        | typing.Iterable[typing.Any,] = EMPTY_VALUE,
    ) -> None:
        self.field: BaseField[typing.Any] = field
        self.operator: type[BaseOperator] = operator

        self.comparison_value: EmptyValue | typing.Any = comparison_value
        self.comparison_values: EmptyValue | typing.Iterable[
            typing.Any,
        ] = comparison_values

    def querystring(self: typing.Self) -> FilterQueryString:
        compare_value: str = ""
        if self.comparison_value is not EMPTY_VALUE:
            if isinstance(self.comparison_value, SQLSelectable):
                compare_value = str(self.comparison_value.querystring())
            else:
                compare_value = transform_value_to_sql(self.comparison_value)
        elif self.comparison_values is not EMPTY_VALUE:
            compare_value = ", ".join(
                [
                    transform_value_to_sql(comparison_value)
                    for comparison_value in self.comparison_values  # type: ignore[union-attr]  # noqa: E501
                ],
            )

        return FilterQueryString(
            self.field.field_name,
            compare_value,
            sql_template=self.operator.operation_template,
        )


class FilterBetween(CombinableExpression):
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

    def querystring(self: typing.Self) -> FilterQueryString:
        left_value: str = (
            self.left_comparison_value.field_name
            if isinstance(self.left_comparison_value, BaseField)
            else transform_value_to_sql(self.left_comparison_value)
        )

        right_value: str = (
            self.right_comparison_value.field_name
            if isinstance(self.right_comparison_value, BaseField)
            else transform_value_to_sql(self.right_comparison_value)
        )

        return FilterQueryString(
            self.field.field_name,
            left_value,
            right_value,
            sql_template=self.operator.operation_template,
        )


class FilterExclusive(CombinableExpression):
    def __init__(
        self: typing.Self,
        comparison: CombinableExpression,
    ) -> None:
        self.comparison: CombinableExpression = comparison

    def querystring(self: typing.Self) -> FilterQueryString:
        return FilterQueryString(
            *self.comparison.querystring().template_arguments,
            sql_template=(
                "(" + self.comparison.querystring().sql_template + ")"
            ),
        )


@dataclasses.dataclass
class FilterStatement(BaseStatement):
    filter_expressions: list[CombinableExpression] = dataclasses.field(
        default_factory=list,
    )

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
        self.filter_expressions.extend(
            where_arguments,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
        if not self.filter_expressions:
            return QueryString.empty()
        final_where: QueryString = functools.reduce(
            operator.add,
            [
                FilterQueryString(  # TODO: find another way to concatenate.
                    *filter_expression.querystring().template_arguments,
                    sql_template=filter_expression.querystring().sql_template,
                )
                for filter_expression in self.filter_expressions
            ],
        )

        return QueryString(
            *final_where.template_arguments,
            sql_template=f"WHERE {final_where.sql_template}",
        )
