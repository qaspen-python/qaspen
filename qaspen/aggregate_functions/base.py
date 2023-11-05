from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from qaspen.base.sql_base import SQLSelectable
from qaspen.querystring.querystring import QueryString
from qaspen.utils.fields_utils import transform_value_to_sql

if TYPE_CHECKING:
    from typing_extensions import Self


class AggFunction(ABC):
    """Main class for all PostgreSQL aggregate function."""

    function_name: str

    def __init__(
        self: Self,
        *func_argument: SQLSelectable | Any,
        alias: str | None = None,
    ) -> None:
        """Initialize AggFunction.

        ### Parameters:
        - `func_argument`: arguments for the aggregate function.
        - `alias`: alias for the function result.
        """
        self.func_arguments: list[SQLSelectable | Any] = [
            *func_argument,
        ]
        self.alias = alias

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`.

        If function argument is `SQLSelectable`
        (usually it's a Field), then use `querystring()`
        method.

        If function argument isn't `SQLSelectable`,
        then try to convert this value into SQL-readable string.

        ### Returns:
        `QueryString` for aggregate function.
        """
        template_args = ", ".join(
            ["{}" for _ in self.func_arguments],
        )
        querystring_args: list[str] = []
        for func_argument in self.func_arguments:
            if isinstance(func_argument, SQLSelectable):
                querystring_args.append(
                    str(func_argument.querystring()),
                )
            else:
                querystring_args.append(
                    transform_value_to_sql(func_argument),
                )
        return QueryString(
            *querystring_args,
            sql_template=self.function_name + "(" + template_args + ")",
        )
