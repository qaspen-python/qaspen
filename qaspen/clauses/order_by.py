from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from qaspen.querystring.querystring import (
    CommaSeparatedQueryString,
    QueryString,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field


class OrderBy:
    """Main class for PostgreSQL OrderBy."""

    def __init__(
        self: Self,
        field: Field[Any],
        ascending: bool | None = None,
        nulls_first: bool | None = None,
    ) -> None:
        self.field: Final[Field[Any]] = field  # type: ignore[arg-type]
        self.ascending: Final = ascending
        self.nulls_first: Final = nulls_first

    def querystring(self: Self) -> CommaSeparatedQueryString:
        """Build `QueryString`."""
        querystring_template = f"{QueryString.arg_ph()}"
        querystring_arguments: list[str] = [self.field.field_name]

        if self.ascending is not None:
            querystring_template += f" {QueryString.arg_ph()}"
            if self.ascending is True:
                querystring_arguments.append("ASC")
            elif self.ascending is False:
                querystring_arguments.append("DESC")

        if self.nulls_first is not None:
            querystring_template += f" {QueryString.arg_ph()}"
            if self.nulls_first is True:
                querystring_arguments.append("NULLS FIRST")
            elif self.nulls_first is False:
                querystring_arguments.append("NULLS LAST")

        return CommaSeparatedQueryString(
            *querystring_arguments,
            sql_template=querystring_template,
        )
