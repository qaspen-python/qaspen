"""Statement result for SelectStatement."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, List, TypeVar

from pydantic import BaseModel, TypeAdapter

from qaspen.statements.statement_result.result_variants import (
    PydanticStatementResult,
    RawStatementResult,
)

if TYPE_CHECKING:
    from typing_extensions import Self

PydanticType = TypeVar(
    "PydanticType",
    bound=BaseModel,
)


class SelectStatementResult(
    RawStatementResult,
    PydanticStatementResult[PydanticType],
):
    """Result for select statement."""

    def __init__(
        self: Self,
        engine_result: list[dict[str, Any]],
        pydantic_model: type[PydanticType] | None = None,
    ) -> None:
        self._engine_result: Final = engine_result
        self._pydantic_model: Final = pydantic_model

    def result(
        self: Self,
    ) -> list[dict[str, Any]]:
        """Return result as-is from engine."""
        return self._engine_result

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
            # ISSUE: https://github.com/qaspen-python/qaspen/issues/41
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
