from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Final, Literal

if TYPE_CHECKING:
    from typing_extensions import Self


class QueryString:
    """QueryString for all statements.

    This class is used for building SQL queries.
    All queries must be build with it.

    `add_delimiter` is for `__add__` method.
    """

    add_delimiter: str = " "
    argument_placeholder: Literal[
        "(__ARG_PLACEHOLDER__)",
    ] = "(__ARG_PLACEHOLDER__)"

    parameter_placeholder: Literal[
        "(__PARAM_PLACEHOLDER__)",
    ] = "(__PARAM_PLACEHOLDER__)"

    def __init__(
        self: Self,
        *template_arguments: Any,
        sql_template: str,
        template_parameters: list[Any] | None = None,
    ) -> None:
        self.sql_template = sql_template
        self.template_arguments: Final = list(template_arguments)
        self.template_parameters: Final = template_parameters or []

        self.template_parameters_count = 1

    @classmethod
    def arg_ph(
        cls: type[QueryString],
    ) -> Literal["(__ARG_PLACEHOLDER__)"]:
        """Return string for argument placeholder.

        For query arguments that must be processed on our side
        we use placeholder for future transformation.

        ### Returns:
        `(__ARG_PLACEHOLDER__)` string
        """
        return cls.argument_placeholder

    @classmethod
    def param_ph(
        cls: type[QueryString],
    ) -> Literal["(__PARAM_PLACEHOLDER__)"]:
        """Return string for parameter placeholder.

        For parameters that must be processed on driver side we
        use placeholder for future transformation.

        ### Returns:
        `(__PARAM_PLACEHOLDER__)` string

        Example:
        -------
        ```
        QueryString(
            "name",
            template_parameters=["Kiselev"],
            sql_template=(
                f"WHERE {QueryString.arg_ph()} "
                f"= {QueryString.param_ph()}"
            )
        )
        ```
        """
        return cls.parameter_placeholder

    @classmethod
    def empty(cls: type[QueryString]) -> EmptyQueryString:
        """Create `EmptyQueryString`.

        :returns: EmptyQueryString.
        """
        return EmptyQueryString(sql_template="")

    def build(
        self: Self,
        engine_type: str = "PSQLPsycopg",
    ) -> str:
        """Build string from querystring.

        Return full SQL querystring with all parameters
        in it.

        In some cases `self.template_arguments` can contain
        other `QueryString`, so we must build them too.

        ### Returns:
        str
        """
        builded_querystring, template_parameters = self._build()
        return self._replace_param_placeholders(
            builded_querystring=builded_querystring,
            engine_type=engine_type,
        )

    def _build(
        self: Self,
        template_parameters: list[Any] | None = None,
    ) -> tuple[str, list[Any]]:
        sql_template = self.sql_template.replace(
            self.argument_placeholder,
            "{}",
        )

        template_arguments = []
        if template_parameters is None:
            template_parameters = []

        for template_argument in self.template_arguments:
            if isinstance(template_argument, QueryString):
                rendered_template, _ = template_argument._build(
                    template_parameters=template_parameters,
                )
                template_arguments.append(rendered_template)
            else:
                template_arguments.append(
                    template_argument,
                )

        template_parameters.extend(
            self.template_parameters,
        )

        return (
            sql_template.format(
                *template_arguments,
            ),
            template_parameters,
        )

    def _replace_param_placeholders(
        self: Self,
        builded_querystring: str,
        engine_type: str,
    ) -> str:
        """Replace parameters placeholders.

        Replace parameters placeholders based on
        engine type.

        For example psycopg needs `%s` for parameters,
        at the same time asyncpg needs $1, $2, ...
        """
        all_params_matches = [
            0
            for _ in re.finditer(
                self.parameter_placeholder,
                builded_querystring,
            )
        ]
        template_parameters_count = 1

        if engine_type == "PSQLPsycopg":
            return builded_querystring.replace(
                self.parameter_placeholder,
                "%s",
            )

        for _ in all_params_matches:
            builded_querystring = builded_querystring.replace(
                self.parameter_placeholder,
                f"${template_parameters_count}",
                1,
            )
            template_parameters_count += 1

        return builded_querystring

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
        self.template_parameters.extend(
            additional_querystring.template_parameters,
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


class CommaSeparatedQueryString(QueryString):
    """QueryString with comma separator."""

    add_delimiter: str = ", "


class FilterQueryString(QueryString):
    """QueryString for FilterStatements like `WHERE`."""

    add_delimiter: str = " AND "


class FullStatementQueryString(QueryString):
    """QueryString for full statements."""

    add_delimiter: str = "; "
