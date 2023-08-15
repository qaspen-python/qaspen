import dataclasses
import functools
import operator
import typing

from qaspen.fields.fields import Field
from qaspen.querystring.querystring import OrderByQueryString, QueryString
from qaspen.statements.statement import BaseStatement


class OrderBy:
    def __init__(
        self: typing.Self,
        field: Field[typing.Any],
        ascending: bool = True,
        nulls_first: bool = True,
    ) -> None:
        self.field: typing.Final[Field[typing.Any]] = field
        self.ascending: typing.Final[bool] = ascending
        self.nulls_first: typing.Final[bool] = nulls_first

    def querystring(self: typing.Self) -> OrderByQueryString:
        querystring_template: typing.Final[str] = "{} {} {}"
        querystring_arguments: list[str] = [self.field.field_name]

        if self.ascending is True:
            querystring_arguments.append("ASC")
        elif self.ascending is False:
            querystring_arguments.append("DESC")

        if self.nulls_first is True:
            querystring_arguments.append("NULLS FIRST")
        elif self.nulls_first is False:
            querystring_arguments.append("NULLS LAST")

        return OrderByQueryString(
            *querystring_arguments,
            sql_template=querystring_template,
        )


@dataclasses.dataclass
class OrderByStatement(BaseStatement):
    order_by_expressions: list[OrderBy] = dataclasses.field(
        default_factory=list,
    )

    def order_by(
        self: typing.Self,
        field: Field[typing.Any] | None = None,
        ascending: bool = True,
        nulls_first: bool = True,
        order_by_statements: typing.Iterable[OrderBy] | None = None,
    ) -> None:
        if field:
            self.order_by_expressions.append(
                OrderBy(
                    field=field,
                    ascending=ascending,
                    nulls_first=nulls_first,
                )
            )

        if order_by_statements:
            self.order_by_expressions.extend(
                order_by_statements,
            )

    def querystring(self: typing.Self) -> QueryString:
        if not self.order_by_expressions:
            return QueryString.empty()
        final_order_by: OrderByQueryString = functools.reduce(
            operator.add,
            [
                order_by_expression.querystring()
                for order_by_expression
                in self.order_by_expressions
            ],
        )

        return QueryString(
            *final_order_by.template_arguments,
            sql_template=f"ORDER BY {final_order_by.sql_template}",
        )