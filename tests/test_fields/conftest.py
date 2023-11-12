"""Conftest for field testing."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Union

from qaspen.fields.base import Field

if TYPE_CHECKING:
    from typing_extensions import Self


def calculate_default_field_value() -> str:
    """Return string as a default value."""
    return "calculated_default_value"


class ForTestField(Field[Union[str, float]]):
    """Field class for testing."""

    _available_comparison_types: tuple[type, ...] = (
        str,
        float,
    )
    _set_available_types: tuple[type, ...] = (
        str,
        float,
    )

    def __init__(
        self: Self,
        *args: Any,  # noqa: ARG002
        is_null: bool = False,
        default: str | float | None | Callable[[], str | float] = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )
