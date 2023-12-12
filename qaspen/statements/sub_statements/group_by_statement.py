from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLSelectable


@dataclass
class GroupByStatement(BaseStatement):
    """GroupBy statement for SelectStatement."""

    group_bys: list[SQLSelectable] = field(
        default_factory=list,
    )

    def group_by(
        self: Self,
        *group_by: SQLSelectable,
    ) -> None:
        """Add new group by arguments.

        ### Parameters:
        - `group_by`: anything with querystring method.
        """
        self.group_bys.extend(group_by)

    def querystring(self: Self) -> QueryString:
        """Build new `QueryString`."""
        if not self.group_bys:
            return QueryString.empty()

        querystring_template: Final = ", ".join(
            [QueryString.arg_ph()] * len(self.group_bys),
        )

        return QueryString(
            *self.group_bys,
            sql_template="GROUP BY " + querystring_template,
        )
