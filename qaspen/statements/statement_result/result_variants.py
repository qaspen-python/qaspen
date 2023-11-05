from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from typing_extensions import Self

ListResultType = TypeVar(
    "ListResultType",
)
ObjectResultType = TypeVar(
    "ObjectResultType",
)


class StatementResult(abc.ABC):
    """Base result for statement.

    Allow result to return raw result.
    As-is from engine.
    """

    @abc.abstractmethod
    def raw_result(
        self: Self,
    ) -> list[tuple[Any, ...]]:
        """Return result as-is from engine."""
        ...


class ListableStatementResult(
    abc.ABC,
    Generic[ListResultType],
):
    """List result.

    Allow to return query result as a string.
    """

    @abc.abstractmethod
    def as_list(self: Self) -> ListResultType:
        """Return results as a list with data."""


class ObjecttableStatementResult(
    abc.ABC,
    Generic[ObjectResultType],
):
    """Object result.

    Allow to return query result as an object/list of objects.
    """

    @abc.abstractmethod
    def as_objects(self: Self) -> list[ObjectResultType]:
        """Return list of objects."""
