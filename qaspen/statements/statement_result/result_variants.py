from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Final, Generic, List, TypeVar

from pydantic import BaseModel, TypeAdapter

if TYPE_CHECKING:
    from typing_extensions import Self

PydanticType = TypeVar(
    "PydanticType",
    bound=BaseModel,
)


class BaseStatementResult(abc.ABC):
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


class StatementResult(BaseStatementResult):
    """Result for statement.

    Must have only one method that allow you to return
    result from engine as-is.
    """

    def __init__(
        self: Self,
        engine_result: list[dict[str, Any]],
    ) -> None:
        """Initialize `StatementResult`.

        ### Parameters:
        - `engine_result`: result from engine.
        """
        self._engine_result: Final = engine_result

    def result(
        self: Self,
    ) -> list[dict[str, Any]]:
        """Return result as-is from engine."""
        return self._engine_result


class PydanticResult(StatementResult, Generic[PydanticType]):
    """Result as a pydantic model."""

    def __init__(
        self: Self,
        engine_result: list[dict[str, Any]],
        pydantic_model: type[PydanticType] | None = None,
    ) -> None:
        super().__init__(engine_result=engine_result)
        self._pydantic_model: Final = pydantic_model

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
        if not self._pydantic_model and not pydantic_model:
            # TO.DO.: NEED TO ALLOW TO CREATE PYDANTIC MODEL IN RUNTIME
            # BY OUR SELVES.
            raise Exception("TODO WOW!")  # noqa: TRY002, EM101

        if pydantic_model:
            type_adapter = TypeAdapter(
                List[pydantic_model],  # type: ignore[valid-type]
            )
            return type_adapter.validate_python(self._engine_result)

        type_adapter = TypeAdapter(
            List[self._pydantic_model],  # type: ignore[name-defined]
        )
        return type_adapter.validate_python(self._engine_result)
