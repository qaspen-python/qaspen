import typing
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression
)
from qaspen.statements.select_statement import SelectStatement


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

    def _to_sql_statement(self: typing.Self) -> str:
        union_operator: str = "UNION ALL" if self.union_all else "UNION"
        return (
            f"{self.left_expression._to_sql_statement()}"
            f" {union_operator} "
            f"{self.right_expression._to_sql_statement()}"
        )


class UnionStatement:
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
        select_expression: SelectStatement,
        union_all: bool = False,
    ) -> typing.Self:
        """"""
        self.union_statement = Union(
            left_expression=self.union_statement,
            right_expression=select_expression,
            union_all=union_all,
        )
        return self

    def build_query(self) -> str:
        return self.union_statement._to_sql_statement()
