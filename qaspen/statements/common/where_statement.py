import typing

from qaspen.fields.comparisons import WhereComparison


class WhereStatementMixin:
    where_statements: list[WhereComparison]

    def where(
        self: typing.Self,
        *where_arguments: WhereComparison,
    ) -> typing.Self:
        print(self.__class__.__dict__)
        self.where_statements.append(
            *where_arguments,
        )
        return self

    def _build_where_query(self: typing.Self) -> str:
        where_clause: str = "WHERE "
        filter_params: str = " AND ".join(
            [
                where_statement.to_sql_statement()
                for where_statement
                in self.where_statements
            ]
        )

        return where_clause + filter_params
