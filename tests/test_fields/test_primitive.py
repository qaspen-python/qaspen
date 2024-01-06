from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from qaspen.base.operators import All_, Any_
from qaspen.exceptions import FieldDeclarationError, FieldValueValidationError
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
    SerialBaseField,
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
    "comparison_value",
    [
        Any_(
            subquery=_ForTestTable.select(_ForTestTable.name),
        ),
        All_(
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
        # ------ SerialField ------
        (SerialField, 12),
        (SerialField, 12.0),
        (SerialField, None),
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
    if issubclass(field, SerialBaseField):

        class TestTable(BaseTable):
            qaspen = field()

    else:

        class TestTable(BaseTable):  # type: ignore[no-redef]
            qaspen = field(is_null=True)

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
    if issubclass(field, SerialBaseField):

        class TestTable(BaseTable):
            qaspen = field()

    else:

        class TestTable(BaseTable):  # type: ignore[no-redef]
            qaspen = field(is_null=True)

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
    assert filter_with_value.left_operand in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == LikeOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "testtable.qaspen LIKE %s"
    assert qs_params == [comparison_value]


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
    assert filter_with_value.left_operand in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == NotLikeOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "testtable.qaspen NOT LIKE %s"
    assert qs_params == [comparison_value]


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
    assert filter_with_value.left_operand in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == ILikeOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "testtable.qaspen ILIKE %s"
    assert qs_params == [comparison_value]


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
    assert filter_with_value.left_operand in [TestTable.qaspen]
    assert filter_with_value.comparison_value == TestTable.qaspen
    assert filter_with_value.operator == NotILikeOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "testtable.qaspen NOT ILIKE %s"
    assert qs_params == [comparison_value]


@pytest.mark.parametrize(
    ("max_length", "value", "expected_exception"),
    [
        (255, "string", None),
        (3, "string", FieldValueValidationError),
    ],
)
def test_varchar_field_validate_field_value_method(
    max_length: int,
    value: str,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `_validate_field_value` method."""

    class TestTable(BaseTable):
        qaspen = VarCharField(max_length=max_length)

    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            TestTable.qaspen._validate_field_value(
                field_value=value,
            )
    else:
        TestTable.qaspen._validate_field_value(
            field_value=value,
        )


def test_varchar_field_field_type_property() -> None:
    """Test `_field_type` property."""

    class TestTable(BaseTable):
        qaspen = VarCharField(max_length=200)

    assert TestTable.qaspen._field_type == "VARCHAR(200)"


def test_char_field_validate_field_value_method() -> None:
    """Test exception in `_validate_field_value` CharField method."""

    class TestTable(BaseTable):
        qaspen = CharField()

    with pytest.raises(expected_exception=FieldValueValidationError):
        TestTable.qaspen._validate_field_value(
            field_value="more_than_one_symbol",
        )


def test_date_field_init_method() -> None:
    """Test `__init__` method.

    Check that it's impossible to declare field
    with `default` and `database_default`.
    """
    with pytest.raises(expected_exception=FieldDeclarationError):
        DateField(
            default=datetime.now().date(),  # noqa: DTZ005
            database_default="CURRENT_DATE",
        )


def test_date_field_field_default_method() -> None:
    """Test `_field_default`.

    Check that return string changes if field has
    `database_default` param.
    """
    field_without_database_default = DateField()
    assert field_without_database_default._field_default == ""

    field_with_database_default = DateField(database_default="CURRENT_DATE")
    assert field_with_database_default._field_default == "DEFAULT CURRENT_DATE"


def test_timestamp_field_init_method() -> None:
    """Test `__init__` method in `TimestampField`.

    Check that it's impossible to declare field
    with `default` and `database_default`.
    """
    with pytest.raises(expected_exception=FieldDeclarationError):
        TimestampField(
            default=datetime.now(),  # noqa: DTZ005
            database_default="CURRENT_DATE",
        )


def test_timestamp_field_field_type_method() -> None:
    """Test `_field_type` in `TimestampField`.

    Check that return string changes if field has
    `with_timezone` param.
    """
    field_without_with_timezone = TimestampField()
    assert field_without_with_timezone._field_type == "TIMESTAMP"

    field_with_timezone = TimestampField(with_timezone=True)
    assert field_with_timezone._field_type == "TIMESTAMP WITH TIME ZONE"
