from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Final, Generator, Generic, TypeVar

from qaspen.utils.engine_utils import find_engine

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction


StatementResultType = TypeVar(
    "StatementResultType",
)


class Executable(ABC, Generic[StatementResultType]):
    """Show that statement can be executed."""

    def __await__(
        self: Self,
    ) -> Generator[None, None, StatementResultType]:
        """Make statement awaitable."""
        engine: Final = find_engine()

        if not engine:
            engine_err_msg: Final = "There is no database engine."
            raise AttributeError(engine_err_msg)

        return self.execute(engine=engine).__await__()

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
