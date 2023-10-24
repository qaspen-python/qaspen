from typing import Final, Union

from typing_extensions import Self

from qaspen.base.sql_base import SQLSelectable
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.statement import BaseStatement


class Intersect(CombinableExpression):
    def __init__(
        self: Self,
        left_expression: Union[SelectStatement[FromTable], "Intersect"],
        right_expression: SelectStatement[FromTable],
    ) -> None:
        self.left_expression: Final = left_expression
        self.right_expression: Final = right_expression

    def querystring(self: Self) -> QueryString:
        return QueryString(
            self.left_expression.querystring(),
            self.right_expression.querystring(),
            sql_template="{} " + "INTERSECT" + " {}",
        )


class IntersectStatement(BaseStatement, SQLSelectable):
    intersect_statement: Intersect

    def __init__(
        self: Self,
        left_expression: Union[SelectStatement[FromTable], Intersect],
        right_expression: SelectStatement[FromTable],
    ) -> None:
        self.intersect_statement: Intersect = Intersect(
            left_expression=left_expression,
            right_expression=right_expression,
        )

    def intersect(
        self: Self,
        select_statement: SelectStatement[FromTable],
    ) -> Self:
        self.intersect_statement = Intersect(
            left_expression=self.intersect_statement,
            right_expression=select_statement,
        )
        return self

    def querystring(self: Self) -> QueryString:
        return self.intersect_statement.querystring()

    def build_query(self: Self) -> str:
        return str(self.querystring())
