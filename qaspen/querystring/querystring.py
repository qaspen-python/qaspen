from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Literal

from qaspen.base.sql_base import SQLSelectable

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
        template_parameters: Iterable[Any] | None = None,
    ) -> None:
        self.sql_template = sql_template
        self.template_arguments = list(template_arguments)
        self.template_parameters = list(template_parameters or [])

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
    ) -> tuple[str, list[Any]]:
        """Build string from querystring.

        Return full SQL querystring with all parameters
        in it.

        In some cases `self.template_arguments` can contain
        other `QueryString`, so we must build them too.

        ### Returns:
        str
        """
        builded_querystring, template_parameters = self._build()
        return (
            self._replace_param_placeholders(
                builded_querystring=builded_querystring,
            ),
            self._preprocess_template_parameters(
                template_parameters=template_parameters,
            ),
        )

    def _preprocess_template_parameters(
        self: Self,
        template_parameters: list[Any],
    ) -> list[Any]:
        """Preprocess template parameters.

        Sometimes this parameters can handle non-basic
        python datatypes, like custom classes, ENUMs,
        qaspen Tables and so on.

        Python database drivers can't process them,
        so we need to preprocess them firstly and
        convert them into driver-readable datatypes.

        ### Parameters:
        - `template_parameters`: query parameters for python db driver.

        ### Returns:
        preprocessed query parameters for python db driver.
        """
        return template_parameters

    def _build(
        self: Self,
        template_parameters: list[Any] | None = None,
    ) -> tuple[str, list[Any]]:
        sql_template = self.sql_template.replace(
            self.argument_placeholder,
            "{}",
        )

        template_arguments: list[str] = []
        if template_parameters is None:
            template_parameters = []

        (
            template_arguments,
            template_parameters,
        ) = self._process_template_arguments(
            template_arguments=template_arguments,
            template_parameters=template_parameters,
        )

        (
            template_arguments,
            template_parameters,
            sql_template,
        ) = self._process_template_parameters(
            template_arguments=template_arguments,
            template_parameters=template_parameters,
            sql_template=sql_template,
        )

        return (
            sql_template.format(
                *template_arguments,
            ),
            template_parameters,
        )

    def _process_template_arguments(
        self: Self,
        template_arguments: list[str],
        template_parameters: list[Any],
    ) -> tuple[list[str], list[Any]]:
        """Process template arguments.

        Template arguments are values that must be processed on the
        qaspen side.
        For example, it can be subclass of Column or AggFunction.

        If template argument is QueryString or something SQLSelectable
        we build it, take `template_arguments` and add built object to it.

        If template argument isn't QueryString or something SQLSelectable
        we just add it as-is to template_arguments.

        ### Parameters:
        - `template_arguments`: built or as-is arguments.
        - `template_parameters`: arguments for driver side render.

        ### Return:
        tuple of `template_arguments` and `template_parameters`.
        """
        for template_argument in self.template_arguments:
            if isinstance(template_argument, QueryString):
                rendered_template, _ = template_argument._build(
                    template_parameters=template_parameters,
                )
                template_arguments.append(rendered_template)
            elif isinstance(template_argument, SQLSelectable):
                rendered_template, _ = template_argument.querystring()._build(
                    template_parameters=template_parameters,
                )
                template_arguments.append(rendered_template)
            else:
                template_arguments.append(
                    template_argument,
                )

        return template_arguments, template_parameters

    def _process_template_parameters(
        self: Self,
        template_arguments: list[str],
        template_parameters: list[Any],
        sql_template: str,
    ) -> tuple[list[str], list[Any], str]:
        """Process template parameters.

        Template parameters are values that must be processed on the
        driver side.
        They can be any default python type (`str`, `int`, `dict`, etc.).

        If template parameters is QueryString or something SQLSelectable
        we build it, take `template_parameters` and add built object to it.

        If template parameters isn't QueryString or something SQLSelectable
        we just add it as-is to template_parameters.

        ### Parameters:
        - `template_arguments`: built or as-is arguments.
        - `template_parameters`: arguments for driver side render.

        ### Return:
        tuple of `template_arguments` and `template_parameters`.
        """
        for template_parameter in self.template_parameters:
            if isinstance(template_parameter, QueryString):
                rendered_template, _ = template_parameter._build(
                    template_parameters=template_parameters,
                )
                if self.parameter_placeholder in sql_template:
                    sql_template = sql_template.replace(
                        self.parameter_placeholder,
                        "{}",
                        1,
                    )
                template_arguments.append(rendered_template)

            elif isinstance(template_parameter, SQLSelectable):
                rendered_template, _ = template_parameter.querystring()._build(
                    template_parameters=template_parameters,
                )
                if self.parameter_placeholder in sql_template:
                    sql_template = sql_template.replace(
                        self.parameter_placeholder,
                        "{}",
                        1,
                    )
                template_arguments.append(rendered_template)

            else:
                template_parameters.append(
                    template_parameter,
                )

        return template_arguments, template_parameters, sql_template

    def _replace_param_placeholders(
        self: Self,
        builded_querystring: str,
    ) -> str:
        """Replace parameters placeholders.

        Replace parameters placeholders based on
        engine type.

        For example psycopg needs `%s` for parameters,
        at the same time asyncpg needs $1, $2, ...
        """
        return builded_querystring.replace(
            self.parameter_placeholder,
            "%s",
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
            "good_column",
            "good_table",
            sql_template="SELECT {} FROM {}",
        )
        qs2 = QueryString(
            "good_column",
            sql_template="ORDER BY {}",
        )
        result_qs = qs1 + qs2
        print(result_qs)
        # SELECT good_column FROM good_table ORDER BY good_column
        ```
        """
        if isinstance(additional_querystring, EmptyQueryString):
            return self

        built_self_qs, built_self_qs_params = self.build()
        additional_qs, additional_qs_params = additional_querystring.build()
        all_qs_params = [*built_self_qs_params, *additional_qs_params]

        self.sql_template = (
            f"{built_self_qs}{self.add_delimiter}{additional_qs}"
        )
        self.template_arguments = []
        self.template_parameters = all_qs_params

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
