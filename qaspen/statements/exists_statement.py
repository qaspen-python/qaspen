import typing
from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.statement import BaseStatement


class ExistsStatement(
    BaseStatement,
    CombinableExpression,
    SQLSelectable,
):
    def __init__(
        self: typing.Self,
        select_statement: SelectStatement,
    ) -> None:
        self.select_statement: typing.Final[
            SelectStatement,
        ] = select_statement

    def querystring(self: typing.Self) -> QueryString:
        return QueryString(
            self.select_statement.querystring(),
            sql_template="EXISTS ({})",
        )

    def querystring_for_select(self: typing.Self) -> QueryString:
        return QueryString(
            self.select_statement.querystring(),
            sql_template="SELECT EXISTS ({})",
        )

    def make_sql_string(self: typing.Self) -> str:
        return str(self.querystring_for_select())
