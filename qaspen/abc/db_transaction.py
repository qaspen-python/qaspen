from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Final, Generic

from qaspen.abc.abc_types import DBConnection, Engine

if TYPE_CHECKING:
    import types

    from typing_extensions import Self


class BaseTransaction(ABC, Generic[Engine, DBConnection]):
    """Base class for all possible database transactions."""

    def __init__(
        self: Self,
        engine: Engine,
    ) -> None:
        self.engine: Final = engine

    @abstractmethod
    async def __aenter__(self: Self) -> Self:
        """Enter in the async context manager.

        This method must setup new transaction.

        ### Returns:
        New transaction context manager.
        """

    @abstractmethod
    async def __aexit__(
        self: Self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        """Close async context manager.

        Must rollback transaction changes if there is an error.

        ### Parameters:
        - `exception_type`: type of the exception.
        - `exception`: instance of the exception.
        - `traceback`: traceback of the exception.
        """

    @abstractmethod
    async def retrieve_connection(self: Self) -> DBConnection:
        """Retrieve new connection.

        Retrieve new connection from the engine.

        ### Returns:
        Connection from the driver.
        """

    @abstractmethod
    async def begin(self: Self) -> None:
        """Start the transaction."""

    @abstractmethod
    async def commit(self: Self) -> None:
        """Commit the transaction."""

    @abstractmethod
    async def rollback(self: Self) -> None:
        """Rollback the transaction."""
