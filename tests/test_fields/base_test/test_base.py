"""Tests for BaseField."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Final

import pytest

from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
from qaspen.fields.base import Field
from qaspen.statements.combinable_statements.filter_statement import (
    EMPTY_VALUE,
)
from qaspen.table.base_table import BaseTable
from tests.test_fields.base_test.conftest import (
    ForTestField,
    calculate_default_field_value,
)

if TYPE_CHECKING:
    from qaspen.statements.combinable_statements.filter_statement import Filter


def test_no_args_in_parameters() -> None:
    """Test that it's impossible to pass not keyword parameters into Field."""
    with pytest.raises(expected_exception=FieldDeclarationError):
        Field("some_arg_argument", "second_arg_argument")


def test_is_null_and_default_params() -> None:
    """Test that is't impossible to have is_null and default parameter."""
    with pytest.raises(expected_exception=FieldDeclarationError):
        Field(is_null=True, default="123")


def test_db_field_name_param() -> None:
    """Test that db_field_name sets field name."""
    field_name: Final = "cool_field_name"
    field = Field[str](db_field_name=field_name)

    assert field._original_field_name == field_name


def test_automate_field_name() -> None:
    """Test that field_name will be set automatically in Table."""

    class ForTestTable(BaseTable):
        field_in_table = Field[str]()

    assert ForTestTable.field_in_table._original_field_name == "field_in_table"


@pytest.mark.parametrize(
    "default_value",
    [
        12,
        ["test", "list"],
        ("test", "tuple"),
        {"test": 1, "dict": 2},
        {"test", "set"},
        frozenset(["test", "frozenset"]),
    ],
)
def test_default_value_validation_failure(
    default_value: Any,
) -> None:
    """Test failure validation of default value for the Field.

    Check that we cannot type for default value that not in
    `_set_available_types`.
    `ForTestField` - testing Field that allows only `str` and `float`
    types for `default`.
    """
    with pytest.raises(expected_exception=FieldValueValidationError):
        ForTestField(default=default_value)


def test_default_value_validation_success() -> None:
    """Test success validation of default value for the Field.

    Check that we can set `str`, `float` and `callable`
    default types to ForTestField.
    """
    field_with_str = ForTestField(default="test_string")
    assert field_with_str._default == "test_string"
    field_with_float = ForTestField(default=12.0)
    assert field_with_float._default == "12.0"

    field_with_callable = ForTestField(default=calculate_default_field_value)
    assert not field_with_callable._default
    assert (
        field_with_callable._field_data.callable_default
        == calculate_default_field_value
    )


@pytest.mark.parametrize(
    (
        "is_null",
        "default",
        "db_field_name",
        "expected_default_value",
        "excepted_default_callable_value",
    ),
    [
        (True, None, "wow_name", None, None),
        (False, None, "wow_name", None, None),
        (False, "string", "wow_name", "string", None),
        (False, 12.0, "wow_name", "12.0", None),
        (
            False,
            calculate_default_field_value,
            "wow_name",
            None,
            calculate_default_field_value,
        ),
    ],
)
def test_field_field_data(
    is_null: bool,
    default: str | float | Callable[[], str | float] | None,
    db_field_name: str,
    expected_default_value: str | None,
    excepted_default_callable_value: Callable[[], str | float] | None,
) -> None:
    """Test full main information about the field."""
    field = ForTestField(
        is_null=is_null,
        default=default,
        db_field_name=db_field_name,
    )

    assert field._is_null == is_null
    assert field._default == expected_default_value
    assert (
        field._field_data.callable_default == excepted_default_callable_value
    )
    assert field._field_data.field_name == db_field_name


def test_field_in_method_with_values(test_for_test_table: BaseTable) -> None:
    """Test `in_` method with comparison_values.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    comparison_values = (
        "search",
        "this",
        "string",
    )
    filter_statement: Final[Filter] = test_for_test_table.name.in_(
        *comparison_values,
    )

    assert filter_statement.comparison_values == comparison_values
    assert filter_statement.comparison_value == EMPTY_VALUE

    querystring = str(filter_statement.querystring())
    assert querystring == ("fortesttable.name IN ('search', 'this', 'string')")


def test_field_in_method_with_subquery(test_for_test_table: BaseTable) -> None:
    """Test `in_` method with subquery.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    subquery = test_for_test_table.select(test_for_test_table.name)
    filter_statement: Final[Filter] = test_for_test_table.name.in_(
        subquery=subquery,
    )

    assert filter_statement.comparison_value == subquery
    assert filter_statement.comparison_values == EMPTY_VALUE

    querystring = str(filter_statement.querystring())
    assert querystring == (
        "fortesttable.name IN (SELECT fortesttable.name FROM fortesttable)"
    )
