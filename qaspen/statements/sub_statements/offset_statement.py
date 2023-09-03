import dataclasses
import typing

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement


@dataclasses.dataclass
class OffsetStatement(BaseStatement):
    offset_number: int | None = None

    def offset(
        self: typing.Self,
        offset_number: int,
    ) -> None:
        self.offset_number: int | None = offset_number

    def querystring(self: typing.Self) -> QueryString:
        if not self.offset_number:
            return QueryString.empty()
        return QueryString(
            self.offset_number,
            sql_template="OFFSET {}",
        )
