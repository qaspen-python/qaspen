import abc
import typing

from qaspen.engine.base import BaseTransaction, SingleConnection
from qaspen.querystring.querystring import QueryString


class Operation(abc.ABC):
    """Base class for all possible operations in migrations."""

    @abc.abstractmethod
    def statement(self: typing.Self) -> QueryString:
        """Return QueryString that can be used in migration."""

    async def execute(
        self: typing.Self,
        transaction: BaseTransaction[SingleConnection],
    ) -> None:
        """Run operation.

        Execute operation in the database.

        :param transaction: transaction instance. It's the same
            for the whole migration.
        """
        await transaction.run_query_without_result(
            querystring=self.statement(),
        )
