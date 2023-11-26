from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generator, Generic, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction


StatementResultType = TypeVar(
    "StatementResultType",
)


class Executable(ABC, Generic[StatementResultType]):
    """Show that statement can be executed."""

    @abstractmethod
    def __await__(
        self: Self,
    ) -> Generator[None, None, StatementResultType]:
        """Make statement awaitable."""

    @abstractmethod
    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> StatementResultType:
        """Execute SQL query and return result."""

    @abstractmethod
    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> StatementResultType:
        """Execute SQL query inside the transaction.

        Than return the result if query can return the result.
        """
