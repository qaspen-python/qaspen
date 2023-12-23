"""Base SQL operators."""
from typing import Final, TYPE_CHECKING

from typing_extensions import Self

if TYPE_CHECKING:
    from qaspen.base.sql_base import SQLSelectable
    from qaspen.querystring.querystring import QueryString


class AnyOperator:
    """ANY PostgreSQL operator.

    Provide functionality of ANY PostgreSQL operator.
    """

    def __init__(
        self: Self,
        subquery: "SQLSelectable",
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

    def querystring(self: Self) -> "QueryString":
        """Build `QueryString` object.

        ### Returns:
        `QueryString`
        """
        subquery_qs = self.subquery.querystring()
        subquery_qs.sql_template = "ANY (" + subquery_qs.sql_template + ")"
        return subquery_qs


class AllOperator:
    """ALL PostgreSQL operator.

    Provide functionality of ALL PostgreSQL operator.
    """

    def __init__(
        self: Self,
        subquery: "SQLSelectable",
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

    def querystring(self: Self) -> "QueryString":
        """Build `QueryString` object.

        ### Returns:
        `QueryString`
        """
        subquery_qs = self.subquery.querystring()
        subquery_qs.sql_template = "ALL (" + subquery_qs.sql_template + ")"
        return subquery_qs
