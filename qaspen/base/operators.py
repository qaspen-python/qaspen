"""Base SQL operators."""
from typing import Final

from typing_extensions import Self

from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString


class AnyOperator(SQLSelectable):
    def __init__(
        self: Self,
        subquery: SQLSelectable,
    ) -> None:
        self.subquery: Final = subquery

    def querystring(self: Self) -> QueryString:
        sql_template: str = (
            "ANY (" + self.subquery.querystring().sql_template + ")"
        )
        return QueryString(
            *self.subquery.querystring().template_arguments,
            sql_template=sql_template,
        )


class AllOperator(SQLSelectable):
    def __init__(
        self: Self,
        subquery: SQLSelectable,
    ) -> None:
        self.subquery: Final = subquery

    def querystring(self: Self) -> QueryString:
        sql_template: str = (
            "ALL (" + self.subquery.querystring().sql_template + ")"
        )
        return QueryString(
            *self.subquery.querystring().template_arguments,
            sql_template=sql_template,
        )
