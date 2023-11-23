from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from qaspen.aggregate_functions.base import AggFunction
from qaspen.utils.fields_utils import transform_value_to_sql

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLSelectable
    from qaspen.clauses.order_by import OrderBy


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

    def __init__(
        self: Self,
        *func_argument: SQLSelectable | Any,
        alias: str | None = None,
    ) -> None:
        """Initialize Coalesce function.

        ### Parameters:
        - `func_argument`: arguments for the aggregate function.
            There is can be unlimited number of function arguments.
        - `alias`: alias for the function result.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )


class Avg(AggFunction):
    """Avg function.

    The `AVG()` function is an aggregate function
    that allows you to get the number of rows
    that match a specific condition of a query.
    """

    function_name = "AVG"

    def __init__(
        self: Self,
        func_argument: SQLSelectable | Any,
        alias: str | None = None,
    ) -> None:
        """Create `AGG` function.

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


class ArrayAgg(AggFunction):
    """ARRAY_AGG function.

    The PostgreSQL ARRAY_AGG() function is an aggregate function
    that accepts a set of values and returns an array
    in which each value in the set is assigned to an element of the array.
    """

    function_name = "ARRAY_AGG"

    def __init__(
        self: Self,
        func_argument: SQLSelectable,
        alias: str | None = None,
        order_by: list[SQLSelectable] | None = None,
        order_by_objs: list[OrderBy] | None = None,
    ) -> None:
        """Create `ARRAY_AGG` function.

        ### Parameters:
        - `func_argument`: arguments for the aggregate function.
            Usually it is Field instance.
        - `alias`: alias for the function result.
        - `order_by`: list of `Field` to order by.
        - `order_by_objs`: list of `OrderBy` objects.
            It can be useful because sometimes you want to add
            `DESC`/`ASC` and `NULLS FIRST/LAST`.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )

        self.order_by: Final = order_by
        self.order_by_objs: Final = order_by_objs

    @property
    def _template_args(self: Self) -> str:
        order_by_args = ""
        order_by_objects_args = ""

        if self.order_by:
            order_by_args = ", ".join(
                ["{}" for _ in self.order_by],
            )
        if self.order_by_objs:
            order_by_objects_args = ", ".join(
                ["{}" for _ in self.order_by_objs],
            )

        if order_by_args and order_by_objects_args:
            return (
                "{} ORDER BY " + f"{order_by_args}, " + order_by_objects_args
            )
        if order_by_args:
            return "{} ORDER BY " + order_by_args
        if order_by_objects_args:
            return "{} ORDER BY " + order_by_objects_args

        return "{}"

    @property
    def _querystring_args(self: Self) -> list[str]:
        querystring_args = super()._querystring_args

        if self.order_by:
            for single_order_by in self.order_by:
                querystring_args.append(
                    str(single_order_by.querystring()),
                )
        if self.order_by_objs:
            for order_by_obj in self.order_by_objs:
                querystring_args.append(
                    str(order_by_obj.querystring()),
                )

        return querystring_args


class Sum(AggFunction):
    """`SUM` function.

    The PostgreSQL SUM() is an aggregate function
    that returns the sum of values or distinct values.
    """

    function_name = "SUM"

    def __init__(
        self: Self,
        func_argument: SQLSelectable,
        alias: str | None = None,
    ) -> None:
        """Create `SUM` function.

        ### Parameters:
        - `func_argument`: It's an object with `querystring()` method
        (Field, for example)
        - `alias`: name for a `AS` clause in statement.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )


class StringAgg(AggFunction):
    """`STRING_AGG` function.

    The PostgreSQL STRING_AGG() function is an aggregate function
    that concatenates a list of strings and places a separator between them.
    The function does not add the separator at the end of the string.
    """

    function_name = "STRING_AGG"

    def __init__(
        self: Self,
        func_argument: SQLSelectable,
        separator: str,
        alias: str | None = None,
        order_by: list[SQLSelectable] | None = None,
        order_by_objs: list[OrderBy] | None = None,
    ) -> None:
        """Create `STRING_AGG` function.

        ### Parameters:
        - `func_argument`: arguments for the aggregate function.
            Tt is Field instance usually.
        - `separator`: separator for strings.
        - `alias`: alias for the function result.
        - `order_by`: list of `Field` to order by.
        - `order_by_objs`: list of `OrderBy` objects.
            It can be useful because sometimes you want to add
            `DESC`/`ASC` and `NULLS FIRST/LAST`.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )

        self.order_by: Final = order_by
        self.order_by_objs: Final = order_by_objs
        self.separator: Final = transform_value_to_sql(separator)

    @property
    def _template_args(self: Self) -> str:
        order_by_args = ""
        order_by_objects_args = ""

        if self.order_by:
            order_by_args = ", ".join(
                ["{}" for _ in self.order_by],
            )
        if self.order_by_objs:
            order_by_objects_args = ", ".join(
                ["{}" for _ in self.order_by_objs],
            )

        if order_by_args and order_by_objects_args:
            return (
                "{}, {} ORDER BY "  # noqa: ISC003
                + f"{order_by_args}, "
                + order_by_objects_args
            )
        if order_by_args:
            return "{}, {} ORDER BY " + order_by_args
        if order_by_objects_args:
            return "{}, {} ORDER BY " + order_by_objects_args

        return "{}, {}"

    @property
    def _querystring_args(self: Self) -> list[str]:
        querystring_args = super()._querystring_args
        querystring_args.append(self.separator)

        if self.order_by:
            for single_order_by in self.order_by:
                querystring_args.append(
                    str(single_order_by.querystring()),
                )
        if self.order_by_objs:
            for order_by_obj in self.order_by_objs:
                querystring_args.append(
                    str(order_by_obj.querystring()),
                )

        return querystring_args


class Max(AggFunction):
    """`MAX` function.

    PostgreSQL MAX function is an aggregate function
    that returns the maximum value in a set of values.
    """

    function_name = "MAX"

    def __init__(
        self: Self,
        func_argument: SQLSelectable,
        alias: str | None = None,
    ) -> None:
        """Create `MAX` function.

        ### Parameters:
        - `func_argument`: It's an object with `querystring()` method
        (Field, for example)
        - `alias`: name for a `AS` clause in statement.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )


class Min(AggFunction):
    """`MIN` function.

    PostgreSQL MIN() function an aggregate function
    that returns the minimum value in a set of values.
    """

    function_name = "MIN"

    def __init__(
        self: Self,
        func_argument: SQLSelectable,
        alias: str | None = None,
    ) -> None:
        """Create `MIN` function.

        ### Parameters:
        - `func_argument`: It's an object with `querystring()` method
        (Field, for example)
        - `alias`: name for a `AS` clause in statement.
        """
        super().__init__(
            func_argument,
            alias=alias,
        )


class Greatest(AggFunction):
    """`GREATEST` function.

    In PostgreSQL, the GREATEST() function returns the largest
    value from the specified values.
    """

    function_name = "GREATEST"

    def __init__(
        self: Self,
        *func_argument: SQLSelectable,
        alias: str | None = None,
    ) -> None:
        """Create `GREATEST` function.

        ### Parameters:
        - `func_argument`: It's an object with `querystring()` method
        (Field, for example)
        - `alias`: name for a `AS` clause in statement.
        """
        super().__init__(
            *func_argument,
            alias=alias,
        )


class Least(AggFunction):
    """`LEAST` function.

    In PostgreSQL, the LEAST() functions returns the smallest
    values from specified values.
    """

    function_name = "LEAST"

    def __init__(
        self: Self,
        *func_argument: SQLSelectable,
        alias: str | None = None,
    ) -> None:
        """Create `LEAST` function.

        ### Parameters:
        - `func_argument`: It's an object with `querystring()` method
        (Field, for example)
        - `alias`: name for a `AS` clause in statement.
        """
        super().__init__(
            *func_argument,
            alias=alias,
        )
