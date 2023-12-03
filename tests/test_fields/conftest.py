"""Conftest for field testing."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Union

import pytest

from qaspen.fields.base import Field
from qaspen.sql_type.primitive_types import VarChar
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
    _sql_type = VarChar

    def __init__(
        self: Self,
        *args: Any,  # noqa: ARG002
        is_null: bool = True,
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
        is_null: bool = True,
        default: int | Callable[[], int] | None = None,
        db_field_name: str | None = None,
    ) -> None:
        super().__init__(
            is_null=is_null,
            default=default,
            db_field_name=db_field_name,
        )


class _ForTestTable(BaseTable):
    """Class for test purposes."""

    name: ForTestField = ForTestField()
    count: ForTestFieldInt = ForTestFieldInt()


@pytest.fixture()
def for_test_table() -> type[_ForTestTable]:
    """Return table class for testing."""

    class ForTestTable(_ForTestTable):
        """Class for test purposes."""

        name: ForTestField = ForTestField(is_null=True)
        count: ForTestFieldInt = ForTestFieldInt(
            is_null=True,
        )

    return ForTestTable
