import typing

from qaspen.fields.comparisons import Where


class WhereStatement:
    where_expressions: list[Where] = []

    def where(
        self: typing.Self,
        *where_arguments: Where,
    ) -> typing.Self:
        self.where_expressions.extend(
            where_arguments,
        )
        return self

    def _build_where_query(self: typing.Self) -> str:
        where_clause: str = "WHERE "
        filter_params: str = " AND ".join(
            [
                where_statement.to_sql_statement()
                for where_statement
                in self.where_expressions
            ]
        )

        return where_clause + filter_params
