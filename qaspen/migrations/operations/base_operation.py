import abc
import typing

from qaspen.engine.base import BaseTransaction, SingleConnection
from qaspen.migrations.inheritance import ClassAsString
from qaspen.querystring.querystring import QueryString


class Operation(ClassAsString, abc.ABC):
    """Base class for all possible operations in migrations."""

    _additional_imports: list[str] = []

    @abc.abstractmethod
    def statement(self: typing.Self) -> QueryString:
        """Return QueryString that can be used in migration."""

    # @abc.abstractmethod
    # def migration_string(self: typing.Self) -> str:
    #     """Convert Operation class into string.

    #     For example:
    #     ------
    #     ```
    #     Operation(field=VarChar(255)).as_string()
    #     ```
    #     will create string like `"Operation(field=VarChar(max_length=255))"`
    #     """

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

    def seialize_parameters(
        self: typing.Self,
        to_serialize_params: dict[str, typing.Any],
    ) -> None:
        pass
