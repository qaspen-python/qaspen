from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString

if TYPE_CHECKING:
    from typing_extensions import Self


class AggFunction(ABC):
    """Main class for all PostgreSQL aggregate function."""

    function_name: str

    def __init__(
        self: Self,
        *func_argument: SQLSelectable | Any,
        alias: str | None = None,
        **_kwargs: Any,
    ) -> None:
        """Initialize AggFunction.

        ### Parameters:
        - `func_argument`: arguments for the aggregate function.
        - `alias`: alias for the function result.
        - `kwargs`: any additional parameters for subclasses.
        """
        self.func_arguments: list[SQLSelectable | Any] = [
            *func_argument,
        ]
        self.alias = alias

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`.

        ### Returns:
        `QueryString` for aggregate function.
        """
        qs_args, qs_params = self._querystring_args_params
        return self._querystring(
            qs_args=qs_args,
            qs_params=qs_params,
            template_args=self._template_args,
        )

    def _querystring(
        self: Self,
        qs_args: list[QueryString],
        qs_params: list[Any],
        template_args: str,
    ) -> QueryString:
        return QueryString(
            *qs_args,
            template_parameters=qs_params,
            sql_template=self.function_name + "(" + template_args + ")",
        )

    @property
    def _template_args(self: Self) -> str:
        result_template_args: list[str] = []
        for func_argument in self.func_arguments:
            if isinstance(func_argument, SQLSelectable):
                result_template_args.append(
                    QueryString.arg_ph(),
                )
            else:
                result_template_args.append(
                    QueryString.param_ph(),
                )

        return ", ".join(result_template_args)

    @property
    def _querystring_args_params(
        self: Self,
    ) -> tuple[list[QueryString], list[Any]]:
        querystring_args: list[QueryString] = []
        querystring_params: list[Any] = []

        for func_argument in self.func_arguments:
            if isinstance(func_argument, SQLSelectable):
                querystring_args.append(
                    func_argument.querystring(),
                )
            else:
                querystring_params.append(func_argument)

        return querystring_args, querystring_params
