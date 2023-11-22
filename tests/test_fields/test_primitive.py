from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
)
from qaspen.fields.primitive import (
    BaseIntegerField,
    BigIntField,
    BigSerialField,
    BooleanField,
    CharField,
    DateField,
    DecimalField,
    DoublePrecisionField,
    IntegerField,
    IntervalField,
    NumericField,
    RealField,
    SerialField,
    SmallIntField,
    SmallSerialField,
    TextField,
    TimeField,
    TimestampField,
    VarCharField,
)
from qaspen.table.base_table import BaseTable
from tests.test_fields.conftest import _ForTestTable

if TYPE_CHECKING:
    from qaspen.fields.base import Field


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


@pytest.mark.parametrize(
    ("field", "comparison_value", "expected_exception"),
    [
        # ------ SmallIntField ------
        (SmallIntField, 12, None),
        (SmallIntField, _ForTestTable.name, None),
        (SmallIntField, {"incorrect": "type"}, FieldComparisonError),
        # ------ IntegerField ------
        (IntegerField, 12, None),
        (IntegerField, _ForTestTable.name, None),
        (IntegerField, {"incorrect": "type"}, FieldComparisonError),
        # ------ BigIntField ------
        (BigIntField, 12, None),
        (BigIntField, _ForTestTable.name, None),
        (BigIntField, {"incorrect": "type"}, FieldComparisonError),
        # ------ NumericField ------
        (NumericField, 12, None),
        (NumericField, _ForTestTable.name, None),
        (NumericField, {"incorrect": "type"}, FieldComparisonError),
        # ------ DecimalField ------
        (DecimalField, 12, None),
        (DecimalField, _ForTestTable.name, None),
        (DecimalField, {"incorrect": "type"}, FieldComparisonError),
        # ------ RealField ------
        (RealField, "12", None),
        (RealField, 12.0, None),
        (RealField, _ForTestTable.name, None),
        (RealField, {"incorrect": "type"}, FieldComparisonError),
        # ------ DoublePrecisionField ------
        (DoublePrecisionField, "12", None),
        (DoublePrecisionField, 12.0, None),
        (DoublePrecisionField, _ForTestTable.name, None),
        (DoublePrecisionField, {"incorrect": "type"}, FieldComparisonError),
        # ------ BooleanField ------
        (BooleanField, True, None),
        (BooleanField, False, None),
        (BooleanField, _ForTestTable.name, None),
        (BooleanField, {"incorrect": "type"}, FieldComparisonError),
        # ------ SmallSerialField ------
        (SmallSerialField, 12, None),
        (SmallSerialField, _ForTestTable.name, None),
        (SmallSerialField, {"incorrect": "type"}, FieldComparisonError),
        # ------ SerialField ------
        (SerialField, 12, None),
        (SerialField, _ForTestTable.name, None),
        (SerialField, {"incorrect": "type"}, FieldComparisonError),
        # ------ BigSerialField ------
        (BigSerialField, 12, None),
        (BigSerialField, _ForTestTable.name, None),
        (BigSerialField, {"incorrect": "type"}, FieldComparisonError),
        # ------ VarCharField ------
        (VarCharField, "string", None),
        (VarCharField, _ForTestTable.name, None),
        (VarCharField, {"incorrect": "type"}, FieldComparisonError),
        # ------ TextField ------
        (TextField, "string", None),
        (TextField, _ForTestTable.name, None),
        (TextField, {"incorrect": "type"}, FieldComparisonError),
        # ------ CharField ------
        (CharField, "string", None),
        (CharField, _ForTestTable.name, None),
        (CharField, {"incorrect": "type"}, FieldComparisonError),
        # ------ DateField ------
        (DateField, datetime.now().date(), None),  # noqa: DTZ005
        (DateField, _ForTestTable.name, None),
        (DateField, {"incorrect": "type"}, FieldComparisonError),
        # ------ TimeField ------
        (TimeField, datetime.now().time(), None),  # noqa: DTZ005
        (TimeField, _ForTestTable.name, None),
        (TimeField, {"incorrect": "type"}, FieldComparisonError),
        # ------ TimestampField ------
        (TimestampField, datetime.now(), None),  # noqa: DTZ005
        (TimestampField, _ForTestTable.name, None),
        (TimestampField, {"incorrect": "type"}, FieldComparisonError),
        # ------ IntervalField ------
        (IntervalField, timedelta(days=1), None),
        (IntervalField, _ForTestTable.name, None),
        (IntervalField, {"incorrect": "type"}, FieldComparisonError),
    ],
)
def test_primitive_field_available_comparison_types(
    field: type[Field[Any]],
    comparison_value: Any,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `_available_comparison_types` with base types."""

    class TestTable(BaseTable):
        qaspen = field()

    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            TestTable.qaspen == comparison_value  # noqa: B015
    else:
        TestTable.qaspen == comparison_value  # noqa: B015


@pytest.mark.parametrize(
    "field",
    [
        SmallIntField,
        IntegerField,
        BigIntField,
        NumericField,
        DecimalField,
        RealField,
        DoublePrecisionField,
        BooleanField,
        SmallSerialField,
        SerialField,
        BigSerialField,
        VarCharField,
        TextField,
        CharField,
        DateField,
        TimeField,
        TimestampField,
        IntervalField,
    ],
)
@pytest.mark.parametrize(
    "comparison_value",
    [
        AnyOperator(
            subquery=_ForTestTable.select(_ForTestTable.name),
        ),
        AllOperator(
            subquery=_ForTestTable.select(_ForTestTable.name),
        ),
    ],
)
def test_primitive_field_available_comparison_types_operator(
    field: type[Field[Any]],
    comparison_value: Any,
) -> None:
    """Test `_available_comparison_types` with operators.

    Check that there is no exception while compare.
    """

    class TestTable(BaseTable):
        qaspen = field()

    TestTable.qaspen == comparison_value  # noqa: B015
