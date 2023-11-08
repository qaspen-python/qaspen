from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Generator, Generic, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine


StatementResultType = TypeVar(
    "StatementResultType",
)


class Executable(abc.ABC, Generic[StatementResultType]):
    """Show that statement can be executed."""

    @abc.abstractmethod
    def __await__(
        self: Self,
    ) -> Generator[None, None, StatementResultType]:
        """Make statement awaitable."""

    @abc.abstractmethod
    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> StatementResultType:
        """Execute SQL query and return result."""
