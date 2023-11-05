from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self


@dataclasses.dataclass
class OffsetStatement(BaseStatement):
    """Offset statement for high-level statements.

    It is used in Select/Update/Insert/Delete Statements.

    `offset_number` offset number.
    """

    offset_number: int | None = None

    def offset(
        self: Self,
        offset_number: int,
    ) -> None:
        """Set offset number.

        ### Parameters:
        - `offset_number`: number of the offset.
        """
        self.offset_number: int | None = offset_number

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        if not self.offset_number:
            return QueryString.empty()
        return QueryString(
            self.offset_number,
            sql_template="OFFSET {}",
        )
