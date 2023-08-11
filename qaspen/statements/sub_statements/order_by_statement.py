import typing

from qaspen.fields.fields import Field


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

    def _to_sql_statement(self: typing.Self) -> str:
        order_by_statement: str = f"{self.field.field_name}"
        if self.ascending is True:
            order_by_statement += " ASC"
        elif self.ascending is False:
            order_by_statement += " DESC"

        if self.nulls_first is True:
            order_by_statement += " NULLS FIRST"
        elif self.nulls_first is False:
            order_by_statement += " NULLS LAST"

        return order_by_statement


class OrderByStatement:
    order_by_expressions: list[OrderBy] = []

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

    def _build_query(self: typing.Self) -> str:
        order_by_params: str = ", ".join(
            order_by_expression._to_sql_statement()
            for order_by_expression
            in self.order_by_expressions
        )
        return "ORDER BY " + order_by_params + " "
