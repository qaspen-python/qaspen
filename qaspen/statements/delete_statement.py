from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Generic

from qaspen.qaspen_types import FromTable
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.filter_statement import (
    FilterStatement,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field


class DeleteStatement(
    BaseStatement,
    Executable[None],
    Generic[FromTable],
):
    """Statement for DELETE queries."""

    def __init__(self: Self, from_table: type[FromTable]) -> None:
        self._from_table: Final = from_table

        self._filter_statement = FilterStatement(
            filter_operator="WHERE",
        )
        self._is_where_used: bool = False
        self._force: bool = False
        self._returning: tuple[Field[Any], ...] | None = None
