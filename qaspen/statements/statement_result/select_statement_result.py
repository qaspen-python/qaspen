"""Statement result for SelectStatement."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from pydantic import TypeAdapter

from qaspen.statements.statement_result.result_variants import (
    MSGSpecStatementResult,
    PydanticStatementResult,
    RawStatementResult,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.qaspen_types import MSGSpecStruct, PydanticModel


class SelectStatementResult(
    RawStatementResult,
    PydanticStatementResult,
    MSGSpecStatementResult,
):
    """Result for select statement."""

    def __init__(
        self: Self,
        engine_result: list[dict[str, Any]],
    ) -> None:
        """Initialize `SelectStatementResult`.

        It is used to get results after statement execution.

        ### Parameters:
        - `engine_result`: result from the engine.
        - `pydantic_model`: pydantic model for engine result.
        """
        super().__init__(engine_result=engine_result)

    def result(
        self: Self,
    ) -> list[dict[str, Any]]:
        """Return result as-is from engine."""
        return self._engine_result

    def to_pydantic(
        self: Self,
        pydantic_model: type[PydanticModel],
    ) -> list[PydanticModel]:
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
        type_adapter = TypeAdapter(
            List[pydantic_model],  # type: ignore[valid-type]
        )
        return type_adapter.validate_python(self._engine_result)

    def to_msgspec(
        self: Self,
        msgspec_struct: type[MSGSpecStruct],
    ) -> list[MSGSpecStruct]:
        """Return result as a list of msgspec structs.

        You can pass the msgspec struct type in the method and
        result will be transformed into you pydantic model.

        ### Parameters:
        - `msgspec_struct`: msgspec struct for engine result.

        ### Returns:
        list of `msgspec_struct`.
        """
        return [
            msgspec_struct(**single_result)
            for single_result in self._engine_result
        ]
