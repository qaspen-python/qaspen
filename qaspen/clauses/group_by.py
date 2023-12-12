from __future__ import annotations

from typing import TYPE_CHECKING, Final

from qaspen.querystring.querystring import QueryString

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLSelectable


class GroupBy:
    """Main class for PostgreSQL GroupBy."""

    def __init__(
        self: Self,
        *group_by: SQLSelectable,
    ) -> None:
        """Create new GROUP BY clause."""
        self.group_by: Final = group_by

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`."""
        querystring_template: Final = ", ".join(
            QueryString.arg_ph() * len(self.group_by),
        )

        return QueryString(
            *self.group_by,
            sql_template=querystring_template,
        )
