import abc
import typing

from qaspen.engine.base_engine import BaseEngine
from qaspen.querystring.querystring import QueryString


class Operation(abc.ABC):
    """Base class for all possible operations in migrations."""

    def __init__(
        self: typing.Self,
    ) -> None:
        pass

    @abc.abstractmethod
    def statement(self: typing.Self) -> QueryString:
        """Return QueryString that can be used in migration."""

    @abc.abstractmethod
    def retrieve_engine(self: typing.Self) -> BaseEngine[typing.Any]:
        """Find engine and use it."""

    async def execute(self: typing.Self) -> None:
        """Run operation.

        Execute operation in the database.
        """
        await self.retrieve_engine().run_query(
            self.statement(),
        )
