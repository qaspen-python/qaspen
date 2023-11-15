"""Conftest for field testing."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Union

import pytest

from qaspen.fields.base import Field
from qaspen.table.base_table import BaseTable

if TYPE_CHECKING:
    from typing_extensions import Self


def calculate_default_field_value() -> str:
    """Return string as a default value."""
    return "calculated_default_value"


class ForTestField(Field[Union[str, float]]):
    """Field class for testing.

    It supports `string` and `float` types.
    """

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


class ForTestFieldInt(Field[int]):
    """Field class for testing.

    It supports `int` type.
    """

    _available_comparison_types: tuple[type, ...] = (int,)
    _set_available_types: tuple[type, ...] = (int,)

    def __init__(
        self: Self,
        *args: Any,  # noqa: ARG002
        is_null: bool = False,
        default: int | Callable[[], int] | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


@pytest.fixture()
def for_test_table() -> type[BaseTable]:
    """Return table class for testing."""

    class ForTestTable(BaseTable):
        """Class for test purposes."""

        name: ForTestField = ForTestField()
        count: ForTestFieldInt = ForTestFieldInt()

    return ForTestTable
