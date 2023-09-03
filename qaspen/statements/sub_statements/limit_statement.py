import dataclasses
import typing

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement


@dataclasses.dataclass
class LimitStatement(BaseStatement):
    limit_number: int | None = None

    def limit(
        self: typing.Self,
        limit_number: int,
    ) -> None:
        self.limit_number = limit_number

    def querystring(self: typing.Self) -> QueryString:
        if not self.limit_number:
            return QueryString.empty()
        return QueryString(
            self.limit_number,
            sql_template="LIMIT {}",
        )
