from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.qaspen_types import PydanticType


class RawStatementResult(abc.ABC):
    """Base class for all result statements.

    It will be used when user perform any query
    that can return the result.
    """

    def __init__(
        self: Self,
        engine_result: list[dict[str, Any]],
    ) -> None:
        """Initialize result statement.

        ### Params:
        - `engine_result`: result from the engine.
        """
        self._engine_result: Final = engine_result

    @abc.abstractmethod
    def result(
        self: Self,
    ) -> list[dict[str, Any]]:
        """Return result as-is from engine.

        As any `Engine` must return result
        as `list[dict[str, Any]]`, this method
        must return just clear engine result.
        """


class PydanticStatementResult(abc.ABC):
    """Result as a pydantic model."""

    @abc.abstractmethod
    def to_pydantic(
        self: Self,
        pydantic_model: type[PydanticType] | None = None,
    ) -> list[PydanticType]:
        """Return result as a pydantic model.

        You can pass the pydantic model in the method and
        result will be transformed into you pydantic model.

        ### Parameters:
        - `pydantic_model`: pydantic model for engine result.

        ### Returns:
        list of `pydantic_models`.
        """
