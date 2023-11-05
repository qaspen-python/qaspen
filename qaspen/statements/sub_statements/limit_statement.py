from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self


@dataclasses.dataclass
class LimitStatement(BaseStatement):
    """Limit statement for high-level statements.

    It is used in Select/Update/Insert/Delete Statements.

    `limit_number` limit number.
    """

    limit_number: int | None = None

    def limit(
        self: Self,
        limit_number: int,
    ) -> None:
        """Set limit number.

        ### Parameters:
        - `limit_number`: number of the limit.
        """
        self.limit_number = limit_number

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        if not self.limit_number:
            return QueryString.empty()
        return QueryString(
            self.limit_number,
            sql_template="LIMIT {}",
        )
