"""Base SQL operators."""
from typing import TYPE_CHECKING, Final

from typing_extensions import Self

if TYPE_CHECKING:
    from qaspen.base.sql_base import SQLSelectable
    from qaspen.querystring.querystring import QueryString


class Any_:  # noqa: N801
    """`ANY` PostgreSQL operator.

    Provide functionality of ANY PostgreSQL operator.
    """

    def __init__(
        self: Self,
        subquery: "SQLSelectable",
    ) -> None:
        """Initialize `Any_`.

        ### Parameters:
        - `subquery`: Any object that provides `querystring()` method.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name == Any_(
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


class All_:  # noqa: N801
    """ALL PostgreSQL operator.

    Provide functionality of ALL PostgreSQL operator.
    """

    def __init__(
        self: Self,
        subquery: "SQLSelectable",
    ) -> None:
        """Initialize `All_`.

        ### Parameters:
        - `subquery`: Any object that provides `querystring()` method.

        Example:
        -------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharColumn = VarCharColumn()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name == All_(
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
