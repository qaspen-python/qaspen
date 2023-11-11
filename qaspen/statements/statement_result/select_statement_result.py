"""Statement result for SelectStatement."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, List

from pydantic import TypeAdapter

from qaspen.statements.statement_result.result_variants import (
    PydanticStatementResult,
    RawStatementResult,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.qaspen_types import PydanticType


class SelectStatementResult(
    RawStatementResult,
    PydanticStatementResult,
):
    """Result for select statement."""

    def __init__(
        self: Self,
        engine_result: list[dict[str, Any]],
        pydantic_model: type[PydanticType] | None = None,
    ) -> None:
        """Initialize `SelectStatementResult`.

        It is used to get results after statement execution.

        ### Parameters:
        - `engine_result`: result from the engine.
        - `pydantic_model`: pydantic model for engine result.
        """
        super().__init__(engine_result=engine_result)
        self._pydantic_model: Final = pydantic_model

    def result(
        self: Self,
    ) -> list[dict[str, Any]]:
        """Return result as-is from engine."""
        return self._engine_result

    def to_pydantic(
        self: Self,
        pydantic_model: type[PydanticType] | None = None,
    ) -> list[PydanticType]:
        """Return result as a pydantic model.

        You can pass the pydantic model in the method and
        result will be transformed into you pydantic model.

        This passed pydantic model will override pydantic model
        that you passed when building statement.

        ### Parameters:
        - `pydantic_model`: pydantic model for engine result.

        ### Returns:
        list of `pydantic_models`.
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
