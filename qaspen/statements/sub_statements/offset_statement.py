import dataclasses
from typing import Optional

from typing_extensions import Self

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement


@dataclasses.dataclass
class OffsetStatement(BaseStatement):
    offset_number: Optional[int] = None

    def offset(
        self: Self,
        offset_number: int,
    ) -> None:
        self.offset_number: Optional[int] = offset_number

    def querystring(self: Self) -> QueryString:
        if not self.offset_number:
            return QueryString.empty()
        return QueryString(
            self.offset_number,
            sql_template="OFFSET {}",
        )
