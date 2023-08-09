import typing

from qaspen.fields.comparisons import CombinableExpression


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
        filter_params: str = " AND".join(
            [
                where_statement.to_sql_statement()
                for where_statement
                in self.where_expressions
            ]
        )

        return "WHERE" + filter_params
