from __future__ import annotations

from typing import TYPE_CHECKING, Any

from qaspen.aggregate_functions.base import AggFunction

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLSelectable


class Count(AggFunction):
    """Count function.

    The `COUNT()` function is an aggregate function
    that allows you to get the number of rows
    that match a specific condition of a query.
    """

    function_name = "COUNT"

    def __init__(
        self: Self,
        func_argument: SQLSelectable | Any,
        alias: str | None = None,
    ) -> None:
        """Create `COUNT` function.

        ### Parameters:
        - `func_argument`: It's an object with `querystring()` method
        (Field, for example), or any base python class
        (like str, int, etc. and their subclasses).
        - `alias`: name for a `AS` clause in statement.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )


class Coalesce(AggFunction):
    """Coalesce function.

    The COALESCE function accepts an unlimited number of arguments.
    It returns the first argument that is not null.
    If all arguments are null, the COALESCE function will return null.
    """

    function_name = "COALESCE"
