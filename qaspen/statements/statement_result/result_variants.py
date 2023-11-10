from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel

if TYPE_CHECKING:
    from typing_extensions import Self

PydanticType = TypeVar(
    "PydanticType",
    bound=BaseModel,
)


class RawStatementResult(abc.ABC):
    """Base class for all result statements.

    It will be used when user perform any query
    that can return the result.
    """

    @abc.abstractmethod
    def result(
        self: Self,
    ) -> list[dict[str, Any]]:
        """Return result as-is from engine.

        As any `Engine` must return result
        as `list[dict[str, Any]]`, this method
        must return just clear engine result.
        """


class PydanticStatementResult(abc.ABC, Generic[PydanticType]):
    """Result as a pydantic model."""

    @abc.abstractmethod
    def as_pydantic(
        self: Self,
        pydantic_model: type[PydanticType] | None = None,
    ) -> list[PydanticType]:
        """Return result as a pydantic model.

        You can pass the pydantic model in the method and
        result will be transformed into you pydantic model.

        This passed pydantic model will override pydantic model
        that you passed when building statement.
        """
