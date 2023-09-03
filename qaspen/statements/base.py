import abc
import typing

from qaspen.engine.base_engine import BaseEngine

StatementResultType = typing.TypeVar(
    "StatementResultType",
)


class Executable(abc.ABC, typing.Generic[StatementResultType]):
    """Show that statement can be executed."""

    @abc.abstractmethod
    def __await__(
        self: typing.Self,
    ) -> typing.Generator[None, None, StatementResultType]:
        """Make statement awaitable."""

    @abc.abstractmethod
    async def execute(
        self: typing.Self,
        engine: BaseEngine[typing.Any],
    ) -> StatementResultType:
        """Execute SQL query and return result."""

    @abc.abstractmethod
    async def _run_query(self: typing.Self) -> StatementResultType:
        """Run query in the engine."""


class ObjectExecutable(Executable[StatementResultType]):
    """Show that the statement can return result as objects."""

    _as_objects: bool = False

    @abc.abstractmethod
    async def execute(
        self: typing.Self,
        engine: BaseEngine[typing.Any],
        as_objects: bool = False,
    ) -> StatementResultType:
        """Execute SQL query and return result."""

    def as_objects(self: typing.Self) -> typing.Self:
        """Set flag that statement must set return result as objects."""
        self._as_objects = True
        return self
