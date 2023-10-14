import abc
import dataclasses
import typing

from qaspen.engine.base import BaseTransaction, SingleConnection
from qaspen.migrations.inheritance import ClassAsString
from qaspen.querystring.querystring import QueryString


@dataclasses.dataclass
class ProcessedParameters:
    parameters: list[str] = dataclasses.field(
        default_factory=list,
    )
    additional_imports: list[str] = dataclasses.field(
        default_factory=list,
    )


class Operation(ClassAsString, abc.ABC):
    """Base class for all possible operations in migrations."""

    _additional_imports: list[str] = []

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
