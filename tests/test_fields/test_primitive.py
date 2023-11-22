from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

import pytest

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
)
from qaspen.fields.operators import (
    ILikeOperator,
    LikeOperator,
    NotILikeOperator,
    NotLikeOperator,
)
from qaspen.fields.primitive import (
    BaseIntegerField,
    BaseStringField,
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
    ("field", "comparison_value"),
    [
        # ------ SmallIntField ------
        (SmallIntField, 12),
        (SmallIntField, 12.0),
        (SmallIntField, _ForTestTable.name),
        # ------ IntegerField ------
        (IntegerField, 12),
        (IntegerField, 12.0),
        (IntegerField, _ForTestTable.name),
        # ------ BigIntField ------
        (BigIntField, 12),
        (BigIntField, 12.0),
        (BigIntField, _ForTestTable.name),
        # ------ NumericField ------
        (NumericField, 12),
        (NumericField, 12.0),
        (NumericField, _ForTestTable.name),
        # ------ DecimalField ------
        (DecimalField, 12),
        (DecimalField, 12.0),
        (DecimalField, _ForTestTable.name),
        # ------ RealField ------
        (RealField, "12"),
        (RealField, 12.0),
        (RealField, 12),
        (RealField, _ForTestTable.name),
        # ------ DoublePrecisionField ------
        (DoublePrecisionField, "12"),
        (DoublePrecisionField, 12.0),
        (DoublePrecisionField, _ForTestTable.name),
        # ------ BooleanField ------
        (BooleanField, True),
        (BooleanField, False),
        (BooleanField, _ForTestTable.name),
        # ------ SmallSerialField ------
        (SmallSerialField, 12),
        (SmallSerialField, 12.0),
        (SmallSerialField, _ForTestTable.name),
        # ------ SerialField ------
        (SerialField, 12),
        (SerialField, 12.0),
        (SerialField, _ForTestTable.name),
        # ------ BigSerialField ------
        (BigSerialField, 12),
        (BigSerialField, 12.0),
        (BigSerialField, _ForTestTable.name),
        # ------ VarCharField ------
        (VarCharField, "string"),
        (VarCharField, _ForTestTable.name),
        # ------ TextField ------
        (TextField, "string"),
        (TextField, _ForTestTable.name),
        # ------ CharField ------
        (CharField, "string"),
        (CharField, _ForTestTable.name),
        # ------ DateField ------
        (DateField, datetime.now().date()),  # noqa: DTZ005
        (DateField, _ForTestTable.name),
        # ------ TimeField ------
        (TimeField, datetime.now().time()),  # noqa: DTZ005
        (TimeField, _ForTestTable.name),
        # ------ TimestampField ------
        (TimestampField, datetime.now()),  # noqa: DTZ005
        (TimestampField, _ForTestTable.name),
        # ------ IntervalField ------
        (IntervalField, timedelta(days=1)),
        (IntervalField, _ForTestTable.name),
    ],
)
def test_primitive_field_available_comparison_types_success(
    field: type[Field[Any]],
    comparison_value: Any,
) -> None:
    """Test `_available_comparison_types` with base types."""

    class TestTable(BaseTable):
        qaspen = field()

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
    "incorrect_comparison_value",
    [
        {"incorrect": "type"},
        {1, 2, 3},
        (1, 2, 3),
        [1, 2, 3],
        frozenset([1, 2, 3]),
    ],
)
def test_primitive_field_available_comparison_types_failure(
    field: type[Field[Any]],
    incorrect_comparison_value: Any,
) -> None:
    """Test `_available_comparison_types` with base types."""

    class TestTable(BaseTable):
        qaspen = field()

    with pytest.raises(expected_exception=FieldComparisonError):
        TestTable.qaspen == incorrect_comparison_value  # noqa: B015


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

    Check that there is no exception.
    """

    class TestTable(BaseTable):
        qaspen = field()

    TestTable.qaspen == comparison_value  # noqa: B015


@pytest.mark.parametrize(
    ("field", "set_value"),
    [
        # ------ SmallIntField ------
        (SmallIntField, 12),
        (SmallIntField, 12.0),
        (SmallIntField, None),
        # ------ IntegerField ------
        (IntegerField, 12),
        (IntegerField, 12.0),
        (IntegerField, None),
        # ------ BigIntField ------
        (BigIntField, 12),
        (BigIntField, 12.0),
        (BigIntField, None),
        # ------ NumericField ------
        (NumericField, 12),
        (NumericField, 12.0),
        (NumericField, None),
        # ------ DecimalField ------
        (DecimalField, 12),
        (DecimalField, 12.0),
        (DecimalField, None),
        # ------ RealField ------
        (RealField, "12"),
        (RealField, 12.0),
        (RealField, 12),
        (RealField, None),
        # ------ DoublePrecisionField ------
        (DoublePrecisionField, "12"),
        (DoublePrecisionField, 12.0),
        (DoublePrecisionField, 12),
        (DoublePrecisionField, None),
        # ------ BooleanField ------
        (BooleanField, True),
        (BooleanField, False),
        (BooleanField, None),
        # ------ SmallSerialField ------
        (SmallSerialField, 12),
        (SmallSerialField, 12.0),
        (SmallSerialField, None),
        # ------ BigSerialField ------
        (BigSerialField, 12),
        (BigSerialField, 12.0),
        (BigSerialField, None),
        # ------ VarCharField ------
        (VarCharField, "string"),
        (VarCharField, None),
        # ------ TextField ------
        (TextField, "string"),
        (TextField, None),
        # ------ CharField ------
        (CharField, "a"),
        (CharField, None),
        # ------ DateField ------
        (DateField, datetime.now().date()),  # noqa: DTZ005
        (DateField, None),
        # ------ TimeField ------
        (TimeField, datetime.now().time()),  # noqa: DTZ005
        (TimeField, None),
        # ------ TimestampField ------
        (TimestampField, datetime.now()),  # noqa: DTZ005
        (TimestampField, None),
        # ------ IntervalField ------
        (IntervalField, timedelta(days=1)),
        (IntervalField, None),
    ],
)
def test_primitive_field_set_available_types_success(
    field: type[Field[Any]],
    set_value: Any,
) -> None:
    """Test `_set_available_types` parameter.

    Check that set value types validate correctly.
    """

    class TestTable(BaseTable):
        qaspen = field()

    table = TestTable()

    # Mustn't raise an exception.
    table.qaspen = set_value


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
    "incorrect_set_value",
    [
        {"incorrect": "type"},
        {1, 2, 3},
        (1, 2, 3),
        [1, 2, 3],
        frozenset([1, 2, 3]),
    ],
)
def test_primitive_field_set_available_types_failure(
    field: type[Field[Any]],
    incorrect_set_value: Any,
) -> None:
    """Test `_set_available_types` with incorrect types."""

    class TestTable(BaseTable):
        qaspen = field()

    table = TestTable()
    with pytest.raises(expected_exception=FieldValueValidationError):
        table.qaspen = incorrect_set_value


@pytest.mark.parametrize(
    ("field", "max_length_value"),
    [
        (VarCharField, 100),
        (VarCharField, 1000),
        (VarCharField, 9999999999999),
    ],
)
def test_str_field_max_length(
    field: type[VarCharField],
    max_length_value: int,
) -> None:
    """Test `max_length` param."""

    class TestTable(BaseTable):
        qaspen: VarCharField = field(max_length=max_length_value)

    assert TestTable.qaspen._max_length == max_length_value


@pytest.mark.parametrize(
    ("field", "comparison_value"),
    [
        (VarCharField, "string"),
        (VarCharField, "%string"),
        (VarCharField, "%string%"),
        (VarCharField, "%string."),
        (TextField, "string"),
        (TextField, "%string"),
        (TextField, "%string%"),
        (TextField, "%string."),
    ],
)
def test_str_field_like_method_success(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `like` method.

    Check that method works fine with correct types.
    """

    class TestTable(BaseTable):
        qaspen = field()

    filter_with_value = TestTable.qaspen.like(
        comparison_value=comparison_value,
    )
    assert filter_with_value.field in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == LikeOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"testtable.qaspen LIKE '{comparison_value}'"


@pytest.mark.parametrize(
    "field",
    [VarCharField, TextField],
)
@pytest.mark.parametrize(
    "comparison_value",
    [
        12,
        12.0,
        {"dict": "is not correct"},
        {"set", "is", "incorrect"},
        ["list", "isn't", "correct"],
        ("tuple", "isn't", "correct"),
        frozenset(("frozenset", "isn't", "correct")),
    ],
)
def test_str_field_like_method_failure(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `like` method.

    Check that method raises an exception with wrong types.
    """

    class TestTable(BaseTable):
        qaspen = field()

    with pytest.raises(expected_exception=FieldComparisonError):
        TestTable.qaspen.like(
            comparison_value=comparison_value,
        )


@pytest.mark.parametrize(
    ("field", "comparison_value"),
    [
        (VarCharField, "string"),
        (VarCharField, "%string"),
        (VarCharField, "%string%"),
        (VarCharField, "%string."),
        (TextField, "string"),
        (TextField, "%string"),
        (TextField, "%string%"),
        (TextField, "%string."),
    ],
)
def test_str_field_not_like_method_success(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `not_like` method."""

    class TestTable(BaseTable):
        qaspen = field()

    filter_with_value = TestTable.qaspen.not_like(
        comparison_value=comparison_value,
    )
    assert filter_with_value.field in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == NotLikeOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"testtable.qaspen NOT LIKE '{comparison_value}'"


@pytest.mark.parametrize(
    "field",
    [VarCharField, TextField],
)
@pytest.mark.parametrize(
    "comparison_value",
    [
        12,
        12.0,
        {"dict": "is not correct"},
        {"set", "is", "incorrect"},
        ["list", "isn't", "correct"],
        ("tuple", "isn't", "correct"),
        frozenset(("frozenset", "isn't", "correct")),
    ],
)
def test_str_field_not_like_method_failure(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `not_like` method.

    Check that method raises an exception with wrong types.
    """

    class TestTable(BaseTable):
        qaspen = field()

    with pytest.raises(expected_exception=FieldComparisonError):
        TestTable.qaspen.not_like(
            comparison_value=comparison_value,
        )


@pytest.mark.parametrize(
    ("field", "comparison_value"),
    [
        (VarCharField, "string"),
        (VarCharField, "%string"),
        (VarCharField, "%string%"),
        (VarCharField, "%string."),
        (TextField, "string"),
        (TextField, "%string"),
        (TextField, "%string%"),
        (TextField, "%string."),
    ],
)
def test_str_field_ilike_method_success(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `ilike` method."""

    class TestTable(BaseTable):
        qaspen = field()

    filter_with_value = TestTable.qaspen.ilike(
        comparison_value=comparison_value,
    )
    assert filter_with_value.field in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == ILikeOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"testtable.qaspen ILIKE '{comparison_value}'"


@pytest.mark.parametrize(
    "field",
    [VarCharField, TextField],
)
@pytest.mark.parametrize(
    "comparison_value",
    [
        12,
        12.0,
        {"dict": "is not correct"},
        {"set", "is", "incorrect"},
        ["list", "isn't", "correct"],
        ("tuple", "isn't", "correct"),
        frozenset(("frozenset", "isn't", "correct")),
    ],
)
def test_str_field_ilike_method_failure(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `ilike` method.

    Check that method raises an exception with wrong types.
    """

    class TestTable(BaseTable):
        qaspen = field()

    with pytest.raises(expected_exception=FieldComparisonError):
        TestTable.qaspen.ilike(
            comparison_value=comparison_value,
        )


@pytest.mark.parametrize(
    ("field", "comparison_value"),
    [
        (VarCharField, "string"),
        (VarCharField, "%string"),
        (VarCharField, "%string%"),
        (VarCharField, "%string."),
        (TextField, "string"),
        (TextField, "%string"),
        (TextField, "%string%"),
        (TextField, "%string."),
    ],
)
def test_str_field_not_ilike_method_success(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `not_ilike` method."""

    class TestTable(BaseTable):
        qaspen = field()

    filter_with_value = TestTable.qaspen.not_ilike(
        comparison_value=comparison_value,
    )
    assert filter_with_value.field in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == NotILikeOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"testtable.qaspen NOT ILIKE '{comparison_value}'"


@pytest.mark.parametrize(
    "field",
    [VarCharField, TextField],
)
@pytest.mark.parametrize(
    "comparison_value",
    [
        12,
        12.0,
        {"dict": "is not correct"},
        {"set", "is", "incorrect"},
        ["list", "isn't", "correct"],
        ("tuple", "isn't", "correct"),
        frozenset(("frozenset", "isn't", "correct")),
    ],
)
def test_str_field_not_ilike_method_failure(
    field: type[BaseStringField],
    comparison_value: str,
) -> None:
    """Test string fields `not_ilike` method.

    Check that method raises an exception with wrong types.
    """

    class TestTable(BaseTable):
        qaspen = field()

    with pytest.raises(expected_exception=FieldComparisonError):
        TestTable.qaspen.not_ilike(
            comparison_value=comparison_value,
        )
