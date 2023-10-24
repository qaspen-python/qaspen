import abc
from typing import Any, Generator, Generic, TypeVar

from typing_extensions import Self

from qaspen.engine.base import BaseEngine

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
        engine: BaseEngine[Any, Any],
    ) -> StatementResultType:
        """Execute SQL query and return result."""

    @abc.abstractmethod
    async def _run_query(self: Self) -> StatementResultType:
        """Run query in the engine."""


class ObjectExecutable(Executable[StatementResultType]):
    """Show that the statement can return result as objects."""

    _as_objects: bool = False

    @abc.abstractmethod
    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any],
    ) -> StatementResultType:
        """Execute SQL query and return result."""

    def as_objects(self: Self) -> Self:
        """Set flag that statement must set return result as objects."""
        self._as_objects = True
        return self
