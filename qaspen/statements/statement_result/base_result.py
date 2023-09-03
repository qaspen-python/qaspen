import abc
import typing


ListResultType = typing.TypeVar(
    "ListResultType",
)
ObjectResultType = typing.TypeVar(
    "ObjectResultType",
)


class RawableStatementResult:
    def __init__(
        self: typing.Self,
        query_results: list[tuple[typing.Any, ...]],
    ) -> None:
        self._query_results: typing.Final = query_results

    def raw_result(
        self: typing.Self,
    ) -> list[tuple[typing.Any, ...]]:
        return self._query_results


class ListableStatementResult(
    abc.ABC,
    typing.Generic[ListResultType],
):
    @abc.abstractmethod
    def as_list(self: typing.Self) -> ListResultType:
        """Return results as a list with data."""


class ObjecttableStatementResult(
    abc.ABC,
    typing.Generic[ObjectResultType],
):
    @abc.abstractmethod
    def as_objects(self: typing.Self) -> list[ObjectResultType]:
        """Return list of objects."""
