"""Base SQL operators."""
from typing import Final

from typing_extensions import Self

from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString


class AnyOperator:
    """ANY PostgreSQL operator.

    Provide functionality of ANY PostgreSQL operator.
    """

    def __init__(
        self: Self,
        subquery: SQLSelectable,
    ) -> None:
        """Initialize AnyOperator.

        ### Parameters:
        - `subquery`: Any object that provides `querystring()` method.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name == AnyOperator(
                    subquery=Buns.select()
                )
            )
        )
        ```
        """
        self.subquery: Final = subquery

    def querystring(self: Self) -> QueryString:
        """Build `QueryString` object.

        ### Returns:
        `QueryString`
        """
        sql_template: str = (
            "ANY (" + self.subquery.querystring().sql_template + ")"
        )
        return QueryString(
            *self.subquery.querystring().template_arguments,
            sql_template=sql_template,
        )


class AllOperator:
    """ALL PostgreSQL operator.

    Provide functionality of ALL PostgreSQL operator.
    """

    def __init__(
        self: Self,
        subquery: SQLSelectable,
    ) -> None:
        """Initialize AllOperator.

        ### Parameters:
        - `subquery`: Any object that provides `querystring()` method.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name == AllOperator(
                    subquery=Buns.select()
                )
            )
        )
        ```
        """
        self.subquery: Final = subquery

    def querystring(self: Self) -> QueryString:
        """Build `QueryString` object.

        ### Returns:
        `QueryString`
        """
        sql_template: str = (
            "ALL (" + self.subquery.querystring().sql_template + ")"
        )
        return QueryString(
            *self.subquery.querystring().template_arguments,
            sql_template=sql_template,
        )
