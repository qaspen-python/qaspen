import typing


class LimitStatement:
    limit_number: int | None = None

    def limit(
        self: typing.Self,
        limit_number: int,
    ) -> None:
        self.limit_number = limit_number

    def _build_query(self: typing.Self) -> str:
        return f"LIMIT {self.limit_number} " if self.limit_number else ""
