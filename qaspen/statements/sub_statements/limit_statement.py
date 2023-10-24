import dataclasses
from typing import Optional

from typing_extensions import Self

from qaspen.querystring.querystring import QueryString
from qaspen.statements.statement import BaseStatement


@dataclasses.dataclass
class LimitStatement(BaseStatement):
    limit_number: Optional[int] = None

    def limit(
        self: Self,
        limit_number: int,
    ) -> None:
        self.limit_number = limit_number

    def querystring(self: Self) -> QueryString:
        if not self.limit_number:
            return QueryString.empty()
        return QueryString(
            self.limit_number,
            sql_template="LIMIT {}",
        )
