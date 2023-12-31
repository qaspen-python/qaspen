from typing import Final

from typing_extensions import Self

from qaspen.base.comparison_operators import AllComparisonMixin
from qaspen.querystring.querystring import QueryString


class Text(AllComparisonMixin[object]):
    """Class for translating python string to database as-is."""

    def __init__(
        self: Self,
        string_value: str,
    ) -> None:
        """Create new Text instance.

        It can be used in situations when you need
        to write raw sql query and execute it or pass
        into some method.

        Qaspen doesn't perform any validation on this
        string, so it will be executed as-is.

        ### Parameters:
        `string_value`: SQL-ready python string.
        """
        self.string_value: Final = string_value

    def querystring(self: Self) -> "QueryString":
        """Create querystring.

        `string_value` must be an argument parameter,
        because driver shouldn't validate it.

        ### Returns:
        new querystring.
        """
        return QueryString(
            self.string_value,
            sql_template=QueryString.arg_ph(),
        )
