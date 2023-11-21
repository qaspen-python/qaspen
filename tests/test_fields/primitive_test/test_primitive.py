from __future__ import annotations

import pytest

from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
from qaspen.fields.primitive import (
    BaseIntegerField,
    BigIntField,
    DecimalField,
    IntegerField,
    NumericField,
    SmallIntField,
)


@pytest.mark.parametrize(
    "field",
    [SmallIntField, IntegerField, BigIntField, NumericField, DecimalField],
)
@pytest.mark.parametrize(
    ("min_value", "max_value", "value", "expected_exception"),
    [
        (None, None, 5, None),
        (None, 10, 5, None),
        (1, 10, 9, None),
        (2, 10, 1, FieldValueValidationError),
        (1, 10, 100, FieldValueValidationError),
    ],
)
def test_integer_field_validate_field_value(
    field: type[BaseIntegerField],
    min_value: int | None,
    max_value: int | None,
    value: int,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `_validate_field_value` in integer fields.

    Check that validation works correct.
    """
    inited_field = field(maximum=max_value, minimum=min_value)
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            inited_field._validate_field_value(
                field_value=value,
            )
    else:
        inited_field._validate_field_value(
            field_value=value,
        )


@pytest.mark.parametrize(
    ("field", "value", "expected_exception"),
    [
        (SmallIntField, 32768, FieldValueValidationError),
        (SmallIntField, 32766, None),
        (IntegerField, 2147483648, FieldValueValidationError),
        (IntegerField, 2147483646, None),
        (BigIntField, 9223372036854775808, FieldValueValidationError),
        (BigIntField, 9223372036854775806, None),
        (NumericField, 9223372036854775808, None),
        (DecimalField, 9223372036854775808, None),
    ],
)
def test_integer_field_available_max_value(
    field: type[BaseIntegerField],
    value: int,
    expected_exception: type[Exception] | None,
) -> None:
    """Test how works _validate_field_value.

    Check that methods correctly checks _available_max_value
    param.
    """
    inited_field = field()
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            inited_field._validate_field_value(
                field_value=value,
            )
    else:
        inited_field._validate_field_value(
            field_value=value,
        )


@pytest.mark.parametrize(
    ("field", "value", "expected_exception"),
    [
        (SmallIntField, -32769, FieldValueValidationError),
        (SmallIntField, -32766, None),
        (IntegerField, -2147483649, FieldValueValidationError),
        (IntegerField, -2147483646, None),
        (BigIntField, -9223372036854775809, FieldValueValidationError),
        (BigIntField, -9223372036854775806, None),
        (NumericField, -9223372036854775808, None),
        (DecimalField, -9223372036854775808, None),
    ],
)
def test_integer_field_available_min_value(
    field: type[BaseIntegerField],
    value: int,
    expected_exception: type[Exception] | None,
) -> None:
    """Test how works _validate_field_value.

    Check that methods correctly checks _available_min_value
    param.
    """
    inited_field = field()
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            inited_field._validate_field_value(
                field_value=value,
            )
    else:
        inited_field._validate_field_value(
            field_value=value,
        )


def test_numeric_init_method() -> None:
    """Test `__init__` numeric method."""
    with pytest.raises(expected_exception=FieldDeclarationError):
        NumericField(scale=2)

    NumericField(
        precision=1,
        scale=2,
        is_null=True,
        db_field_name="wow",
    )


@pytest.mark.parametrize(
    ("precision", "scale", "str_field_type"),
    [
        (2, None, "NUMERIC(2)"),
        (2, 2, "NUMERIC(2, 2)"),
        (None, None, "NUMERIC"),
    ],
)
def test_numeric_field_type(
    precision: int | None,
    scale: int | None,
    str_field_type: str,
) -> None:
    """Test `_field_type` numeric method."""
    field = NumericField(precision=precision, scale=scale)
    assert field._field_type == str_field_type
