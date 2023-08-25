import dataclasses
import functools
import operator
import typing
from qaspen.base.sql_base import SQLSelectable
from qaspen.fields.base.base_field import BaseField


from qaspen.fields.operators import BaseOperator
from qaspen.querystring.querystring import QueryString, WhereQueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression
)
from qaspen.statements.statement import BaseStatement


class EmptyValue:
    pass


EMPTY_VALUE = EmptyValue()


class Where(CombinableExpression):
    def __init__(
        self: typing.Self,
        field: BaseField[typing.Any],
        operator: type[BaseOperator],
        comparison_value: (
            EmptyValue | BaseField[typing.Any] | typing.Any
        ) = EMPTY_VALUE,
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

    def querystring(self: typing.Self) -> WhereQueryString:
        if self.comparison_value is not EMPTY_VALUE:
            if isinstance(self.comparison_value, SQLSelectable):
                compare_value: str = self.comparison_value.make_sql_string()
            else:
                compare_value = f"'{self.comparison_value}'"
        elif self.comparison_values is not EMPTY_VALUE:
            compare_value = ", ".join(
                [
                    f"'{comparison_value}'"
                    for comparison_value
                    in self.comparison_values  # type: ignore[union-attr]
                ]
            )

        return WhereQueryString(
            self.field.field_name_with_prefix,
            compare_value,
            sql_template=self.operator.operation_template,
        )


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

    def querystring(self: typing.Self) -> WhereQueryString:
        return WhereQueryString(
            self.field.field_name_with_prefix,
            self.left_comparison_value,
            self.right_comparison_value,
            sql_template=self.operator.operation_template,
        )


class WhereExclusive(CombinableExpression):

    def __init__(
        self: typing.Self,
        comparison: CombinableExpression,
    ) -> None:
        self.comparison: CombinableExpression = comparison

    def querystring(self: typing.Self) -> WhereQueryString:
        return WhereQueryString(
            *self.comparison.querystring().template_arguments,
            sql_template=(
                "(" + self.comparison.querystring().sql_template + ")"
            ),
        )


@dataclasses.dataclass
class WhereStatement(BaseStatement):
    where_expressions: list[CombinableExpression] = dataclasses.field(
        default_factory=list,
    )

    def where(
        self: typing.Self,
        *where_arguments: CombinableExpression,
    ) -> typing.Self:
        self.where_expressions.extend(
            where_arguments,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
        if not self.where_expressions:
            return QueryString.empty()
        final_where: QueryString = functools.reduce(
            operator.add,
            [
                WhereQueryString(  # TODO: find another way to concatenate.
                    *where_expression.querystring().template_arguments,
                    sql_template=where_expression.querystring().sql_template,
                )
                for where_expression
                in self.where_expressions
            ],
        )

        return QueryString(
            *final_where.template_arguments,
            sql_template=f"WHERE {final_where.sql_template}",
        )
