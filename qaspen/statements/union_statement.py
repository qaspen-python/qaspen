import typing
from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.statement import BaseStatement


class Union(CombinableExpression):
    def __init__(
        self: typing.Self,
        left_expression: "SelectStatement | Union",
        right_expression: SelectStatement,
        union_all: bool = False,
    ) -> None:
        self.left_expression: SelectStatement | "Union" = left_expression
        self.right_expression: SelectStatement = right_expression
        self.union_all: bool = union_all

    def querystring(self: typing.Self) -> QueryString:
        union_operator: str = "UNION ALL" if self.union_all else "UNION"
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + union_operator + " {}"
        )


class UnionStatement(BaseStatement, SQLSelectable):
    union_statement: Union

    def __init__(
        self: typing.Self,
        left_expression: SelectStatement | Union,
        right_expression: SelectStatement,
        union_all: bool = False,
    ) -> None:
        self.union_statement: Union = Union(
            left_expression=left_expression,
            right_expression=right_expression,
            union_all=union_all,
        )

    def union(
        self: typing.Self,
        select_statement: SelectStatement,
        union_all: bool = False,
    ) -> typing.Self:
        self.union_statement = Union(
            left_expression=self.union_statement,
            right_expression=select_statement,
            union_all=union_all,
        )
        return self

    def querystring(self: typing.Self) -> QueryString:
        return self.union_statement.querystring()

    def build_query(self: typing.Self) -> str:
        return str(self.querystring())
