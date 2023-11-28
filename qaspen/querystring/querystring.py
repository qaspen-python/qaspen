from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing_extensions import Self


class QueryString:
    """QueryString for all statements.

    This class is used for building SQL queries.
    All queries must be build with it.

    `add_delimiter` is for `__add__` method.
    """

    add_delimiter: str = " "

    def __init__(
        self: Self,
        *template_arguments: Any,
        sql_template: str,
    ) -> None:
        self.sql_template: str = sql_template
        self.template_arguments: list[Any] = list(template_arguments)

    @classmethod
    def empty(cls: type[QueryString]) -> EmptyQueryString:
        """Create `EmptyQueryString`.

        :returns: EmptyQueryString.
        """
        return EmptyQueryString(sql_template="")

    def build(self: Self) -> str:
        """Build string from querystring.

        Return full SQL querystring with all parameters
        in it.

        In some cases `self.template_arguments` can contain
        other `QueryString`, so we must build them too.

        ### Returns:
        str
        """
        template_arguments = []
        for template_argument in self.template_arguments:
            if isinstance(template_argument, QueryString):
                template_arguments.append(
                    template_argument.build(),
                )
            else:
                template_arguments.append(
                    template_argument,
                )

        return self.sql_template.format(
            *template_arguments,
        )

    def __add__(
        self: Self,
        additional_querystring: QueryString,
    ) -> Self:
        """Combine two QueryStrings.

        ### Parameters
        :param `additional_querystring`: second QueryString.

        ### Returns
        :returns: self.

        Example:
        -------
        ```python
        qs1 = QueryString(
            "good_field",
            "good_table",
            sql_template="SELECT {} FROM {}",
        )
        qs2 = QueryString(
            "good_field",
            sql_template="ORDER BY {}",
        )
        result_qs = qs1 + qs2
        print(result_qs)
        # SELECT good_field FROM good_table ORDER BY good_field
        ```
        """
        if isinstance(additional_querystring, EmptyQueryString):
            return self

        self.sql_template += (
            f"{self.add_delimiter}{additional_querystring.sql_template}"
        )
        self.template_arguments.extend(
            additional_querystring.template_arguments,
        )
        return self

    def __str__(self: Self) -> str:
        """Return `QueryString` as a sql template without data.

        ### Returns
        :returns: string
        """
        return self.sql_template


class EmptyQueryString(QueryString):
    """QueryString without data inside."""

    add_delimiter: str = ""


class OrderByQueryString(QueryString):
    """QueryString for OrderBy clauses."""

    add_delimiter: str = ", "


class FilterQueryString(QueryString):
    """QueryString for FilterStatements like `WHERE`."""

    add_delimiter: str = " AND "


class FullStatementQueryString(QueryString):
    """QueryString for full statements."""

    add_delimiter: str = "; "
