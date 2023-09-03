import typing

from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.statement import BaseStatement


class Intersect(CombinableExpression):
    def __init__(
        self: typing.Self,
        left_expression: "SelectStatement | Intersect",
        right_expression: SelectStatement,
    ) -> None:
        self.left_expression: SelectStatement | "Intersect" = left_expression
        self.right_expression: SelectStatement = right_expression

    def querystring(self: typing.Self) -> QueryString:
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + "INTERSECT" + " {}",
        )


class IntersectStatement(BaseStatement, SQLSelectable):
    intersect_statement: Intersect

    def __init__(
        self: typing.Self,
        left_expression: SelectStatement | Intersect,
        right_expression: SelectStatement,
    ) -> None:
        self.intersect_statement: Intersect = Intersect(
            left_expression=left_expression,
            right_expression=right_expression,
        )

    def intersect(
        self: typing.Self,
        select_statement: SelectStatement,
    ) -> typing.Self:
        self.intersect_statement = Intersect(
            left_expression=self.intersect_statement,
            right_expression=select_statement,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
        return self.intersect_statement.querystring()

    def build_query(self: typing.Self) -> str:
        return str(self.querystring())
