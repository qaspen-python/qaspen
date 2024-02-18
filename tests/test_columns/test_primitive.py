from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from qaspen.base.operators import All_, Any_
from qaspen.columns.operators import (
    ILikeOperator,
    LikeOperator,
    NotILikeOperator,
    NotLikeOperator,
)
from qaspen.columns.primitive import (
    BaseIntegerColumn,
    BaseStringColumn,
    BigIntColumn,
    BigSerialColumn,
    BooleanColumn,
    CharColumn,
    DateColumn,
    DecimalColumn,
    DoublePrecisionColumn,
    IntegerColumn,
    IntervalColumn,
    NumericColumn,
    RealColumn,
    SerialBaseColumn,
    SerialColumn,
    SmallIntColumn,
    SmallSerialColumn,
    TextColumn,
    TimeColumn,
    TimestampColumn,
    TimestampTZColumn,
    TimeTZColumn,
    VarCharColumn,
)
from qaspen.exceptions import (
    ColumnDeclarationError,
    ColumnValueValidationError,
)
from qaspen.table.base_table import BaseTable
from tests.test_columns.conftest import _ForTestTable

if TYPE_CHECKING:
    from qaspen.columns.base import Column


@pytest.mark.parametrize(
    "column",
    [
        SmallIntColumn,
        IntegerColumn,
        BigIntColumn,
        NumericColumn,
        DecimalColumn,
    ],
)
@pytest.mark.parametrize(
    ("min_value", "max_value", "value", "expected_exception"),
    [
        (None, None, 5, None),
        (None, 10, 5, None),
        (1, 10, 9, None),
        (2, 10, 1, ColumnValueValidationError),
        (1, 10, 100, ColumnValueValidationError),
    ],
)
def test_integer_column_validate_column_value(
    column: type[BaseIntegerColumn],
    min_value: int | None,
    max_value: int | None,
    value: int,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `_validate_column_value` in integer columns.

    Check that validation works correct.
    """
    inited_column = column(maximum=max_value, minimum=min_value)
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            inited_column._validate_column_value(
                column_value=value,
            )
    else:
        inited_column._validate_column_value(
            column_value=value,
        )


@pytest.mark.parametrize(
    ("column", "value", "expected_exception"),
    [
        (SmallIntColumn, 32768, ColumnValueValidationError),
        (SmallIntColumn, 32766, None),
        (IntegerColumn, 2147483648, ColumnValueValidationError),
        (IntegerColumn, 2147483646, None),
        (BigIntColumn, 9223372036854775808, ColumnValueValidationError),
        (BigIntColumn, 9223372036854775806, None),
        (NumericColumn, 9223372036854775808, None),
        (DecimalColumn, 9223372036854775808, None),
    ],
)
def test_integer_column_available_max_value(
    column: type[BaseIntegerColumn],
    value: int,
    expected_exception: type[Exception] | None,
) -> None:
    """Test how works _validate_column_value.

    Check that methods correctly checks _available_max_value
    param.
    """
    inited_column = column()
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            inited_column._validate_column_value(
                column_value=value,
            )
    else:
        inited_column._validate_column_value(
            column_value=value,
        )


@pytest.mark.parametrize(
    ("column", "value", "expected_exception"),
    [
        (SmallIntColumn, -32769, ColumnValueValidationError),
        (SmallIntColumn, -32766, None),
        (IntegerColumn, -2147483649, ColumnValueValidationError),
        (IntegerColumn, -2147483646, None),
        (BigIntColumn, -9223372036854775809, ColumnValueValidationError),
        (BigIntColumn, -9223372036854775806, None),
        (NumericColumn, -9223372036854775808, None),
        (DecimalColumn, -9223372036854775808, None),
    ],
)
def test_integer_column_available_min_value(
    column: type[BaseIntegerColumn],
    value: int,
    expected_exception: type[Exception] | None,
) -> None:
    """Test how works _validate_column_value.

    Check that methods correctly checks _available_min_value
    param.
    """
    inited_column = column()
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            inited_column._validate_column_value(
                column_value=value,
            )
    else:
        inited_column._validate_column_value(
            column_value=value,
        )


def test_numeric_init_method() -> None:
    """Test `__init__` numeric method."""
    with pytest.raises(expected_exception=ColumnDeclarationError):
        NumericColumn(scale=2)

    NumericColumn(
        precision=1,
        scale=2,
        is_null=True,
        db_column_name="wow",
    )


@pytest.mark.parametrize(
    ("precision", "scale", "str_column_type"),
    [
        (2, None, "NUMERIC(2)"),
        (2, 2, "NUMERIC(2, 2)"),
        (None, None, "NUMERIC"),
    ],
)
def test_numeric_column_type(
    precision: int | None,
    scale: int | None,
    str_column_type: str,
) -> None:
    """Test `_column_type` numeric method."""
    column = NumericColumn(precision=precision, scale=scale)
    assert column._column_type == str_column_type


@pytest.mark.parametrize(
    ("column", "comparison_value"),
    [
        # ------ SmallIntColumn ------
        (SmallIntColumn, 12),
        (SmallIntColumn, 12.0),
        (SmallIntColumn, _ForTestTable.name),
        # ------ IntegerColumn ------
        (IntegerColumn, 12),
        (IntegerColumn, 12.0),
        (IntegerColumn, _ForTestTable.name),
        # ------ BigIntColumn ------
        (BigIntColumn, 12),
        (BigIntColumn, 12.0),
        (BigIntColumn, _ForTestTable.name),
        # ------ NumericColumn ------
        (NumericColumn, 12),
        (NumericColumn, 12.0),
        (NumericColumn, _ForTestTable.name),
        # ------ DecimalColumn ------
        (DecimalColumn, 12),
        (DecimalColumn, 12.0),
        (DecimalColumn, _ForTestTable.name),
        # ------ RealColumn ------
        (RealColumn, "12"),
        (RealColumn, 12.0),
        (RealColumn, 12),
        (RealColumn, _ForTestTable.name),
        # ------ DoublePrecisionColumn ------
        (DoublePrecisionColumn, "12"),
        (DoublePrecisionColumn, 12.0),
        (DoublePrecisionColumn, _ForTestTable.name),
        # ------ BooleanColumn ------
        (BooleanColumn, True),
        (BooleanColumn, False),
        (BooleanColumn, _ForTestTable.name),
        # ------ SmallSerialColumn ------
        (SmallSerialColumn, 12),
        (SmallSerialColumn, 12.0),
        (SmallSerialColumn, _ForTestTable.name),
        # ------ SerialColumn ------
        (SerialColumn, 12),
        (SerialColumn, 12.0),
        (SerialColumn, _ForTestTable.name),
        # ------ BigSerialColumn ------
        (BigSerialColumn, 12),
        (BigSerialColumn, 12.0),
        (BigSerialColumn, _ForTestTable.name),
        # ------ VarCharColumn ------
        (VarCharColumn, "string"),
        (VarCharColumn, _ForTestTable.name),
        # ------ TextColumn ------
        (TextColumn, "string"),
        (TextColumn, _ForTestTable.name),
        # ------ CharColumn ------
        (CharColumn, "string"),
        (CharColumn, _ForTestTable.name),
        # ------ DateColumn ------
        (DateColumn, datetime.now().date()),  # noqa: DTZ005
        (DateColumn, _ForTestTable.name),
        # ------ TimeColumn ------
        (TimeColumn, datetime.now().time()),  # noqa: DTZ005
        (TimeColumn, _ForTestTable.name),
        # ------ TimeTZColumn ------
        (TimeTZColumn, datetime.now().time()),  # noqa: DTZ005
        (TimeTZColumn, _ForTestTable.name),
        # ------ TimestampColumn ------
        (TimestampColumn, datetime.now()),  # noqa: DTZ005
        (TimestampColumn, _ForTestTable.name),
        # ------ TimestampTZColumn ------
        (TimestampTZColumn, datetime.now()),  # noqa: DTZ005
        (TimestampTZColumn, _ForTestTable.name),
        # ------ IntervalColumn ------
        (IntervalColumn, timedelta(days=1)),
        (IntervalColumn, _ForTestTable.name),
    ],
)
def test_primitive_column_available_comparison_types_success(
    column: type[Column[Any]],
    comparison_value: Any,
) -> None:
    """Test `_available_comparison_types` with base types."""

    class TestTable(BaseTable):
        qaspen = column()

    TestTable.qaspen == comparison_value  # noqa: B015


@pytest.mark.parametrize(
    "column",
    [
        SmallIntColumn,
        IntegerColumn,
        BigIntColumn,
        NumericColumn,
        DecimalColumn,
        RealColumn,
        DoublePrecisionColumn,
        BooleanColumn,
        SmallSerialColumn,
        SerialColumn,
        BigSerialColumn,
        VarCharColumn,
        TextColumn,
        CharColumn,
        DateColumn,
        TimeColumn,
        TimeTZColumn,
        TimestampColumn,
        TimestampTZColumn,
        IntervalColumn,
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
def test_primitive_column_available_comparison_types_operator(
    column: type[Column[Any]],
    comparison_value: Any,
) -> None:
    """Test `_available_comparison_types` with operators.

    Check that there is no exception.
    """

    class TestTable(BaseTable):
        qaspen = column()

    TestTable.qaspen == comparison_value  # noqa: B015


@pytest.mark.parametrize(
    ("column", "set_value"),
    [
        # ------ SmallIntColumn ------
        (SmallIntColumn, 12),
        (SmallIntColumn, 12.0),
        (SmallIntColumn, None),
        # ------ IntegerColumn ------
        (IntegerColumn, 12),
        (IntegerColumn, 12.0),
        (IntegerColumn, None),
        # ------ BigIntColumn ------
        (BigIntColumn, 12),
        (BigIntColumn, 12.0),
        (BigIntColumn, None),
        # ------ NumericColumn ------
        (NumericColumn, 12),
        (NumericColumn, 12.0),
        (NumericColumn, None),
        # ------ DecimalColumn ------
        (DecimalColumn, 12),
        (DecimalColumn, 12.0),
        (DecimalColumn, None),
        # ------ RealColumn ------
        (RealColumn, "12"),
        (RealColumn, 12.0),
        (RealColumn, 12),
        (RealColumn, None),
        # ------ DoublePrecisionColumn ------
        (DoublePrecisionColumn, "12"),
        (DoublePrecisionColumn, 12.0),
        (DoublePrecisionColumn, 12),
        (DoublePrecisionColumn, None),
        # ------ BooleanColumn ------
        (BooleanColumn, True),
        (BooleanColumn, False),
        (BooleanColumn, None),
        # ------ SmallSerialColumn ------
        (SmallSerialColumn, 12),
        (SmallSerialColumn, 12.0),
        (SmallSerialColumn, None),
        # ------ SerialColumn ------
        (SerialColumn, 12),
        (SerialColumn, 12.0),
        (SerialColumn, None),
        # ------ BigSerialColumn ------
        (BigSerialColumn, 12),
        (BigSerialColumn, 12.0),
        (BigSerialColumn, None),
        # ------ VarCharColumn ------
        (VarCharColumn, "string"),
        (VarCharColumn, None),
        # ------ TextColumn ------
        (TextColumn, "string"),
        (TextColumn, None),
        # ------ CharColumn ------
        (CharColumn, "a"),
        (CharColumn, None),
        # ------ DateColumn ------
        (DateColumn, datetime.now().date()),  # noqa: DTZ005
        (DateColumn, None),
        # ------ TimeColumn ------
        (TimeColumn, datetime.now().time()),  # noqa: DTZ005
        (TimeColumn, None),
        # ------ TimeTZColumn ------
        (TimeTZColumn, datetime.now().time()),  # noqa: DTZ005
        (TimeTZColumn, None),
        # ------ TimestampColumn ------
        (TimestampColumn, datetime.now()),  # noqa: DTZ005
        (TimestampColumn, None),
        # ------ TimestampTZColumn ------
        (TimestampTZColumn, datetime.now()),  # noqa: DTZ005
        (TimestampTZColumn, None),
        # ------ IntervalColumn ------
        (IntervalColumn, timedelta(days=1)),
        (IntervalColumn, None),
    ],
)
def test_primitive_column_set_available_types_success(
    column: type[Column[Any]],
    set_value: Any,
) -> None:
    """Test `_set_available_types` parameter.

    Check that set value types validate correctly.
    """
    if issubclass(column, SerialBaseColumn):

        class TestTable(BaseTable):
            qaspen = column()

    else:

        class TestTable(BaseTable):  # type: ignore[no-redef]
            qaspen = column(is_null=True)

    table = TestTable()

    # Mustn't raise an exception.
    table.qaspen = set_value


@pytest.mark.parametrize(
    "column",
    [
        SmallIntColumn,
        IntegerColumn,
        BigIntColumn,
        NumericColumn,
        DecimalColumn,
        RealColumn,
        DoublePrecisionColumn,
        BooleanColumn,
        SmallSerialColumn,
        SerialColumn,
        BigSerialColumn,
        VarCharColumn,
        TextColumn,
        CharColumn,
        DateColumn,
        TimeColumn,
        TimestampColumn,
        IntervalColumn,
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
def test_primitive_column_set_available_types_failure(
    column: type[Column[Any]],
    incorrect_set_value: Any,
) -> None:
    """Test `_set_available_types` with incorrect types."""
    if issubclass(column, SerialBaseColumn):

        class TestTable(BaseTable):
            qaspen = column()

    else:

        class TestTable(BaseTable):  # type: ignore[no-redef]
            qaspen = column(is_null=True)

    table = TestTable()
    with pytest.raises(expected_exception=ColumnValueValidationError):
        table.qaspen = incorrect_set_value


@pytest.mark.parametrize(
    ("column", "max_length_value"),
    [
        (VarCharColumn, 100),
        (VarCharColumn, 1000),
        (VarCharColumn, 9999999999999),
    ],
)
def test_str_column_max_length(
    column: type[VarCharColumn],
    max_length_value: int,
) -> None:
    """Test `max_length` param."""

    class TestTable(BaseTable):
        qaspen: VarCharColumn = column(max_length=max_length_value)

    assert TestTable.qaspen._max_length == max_length_value


@pytest.mark.parametrize(
    ("column", "comparison_value"),
    [
        (VarCharColumn, "string"),
        (VarCharColumn, "%string"),
        (VarCharColumn, "%string%"),
        (VarCharColumn, "%string."),
        (TextColumn, "string"),
        (TextColumn, "%string"),
        (TextColumn, "%string%"),
        (TextColumn, "%string."),
    ],
)
def test_str_column_like_method_success(
    column: type[BaseStringColumn],
    comparison_value: str,
) -> None:
    """Test string columns `like` method.

    Check that method works fine with correct types.
    """

    class TestTable(BaseTable):
        qaspen = column()

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
    ("column", "comparison_value"),
    [
        (VarCharColumn, "string"),
        (VarCharColumn, "%string"),
        (VarCharColumn, "%string%"),
        (VarCharColumn, "%string."),
        (TextColumn, "string"),
        (TextColumn, "%string"),
        (TextColumn, "%string%"),
        (TextColumn, "%string."),
    ],
)
def test_str_column_not_like_method_success(
    column: type[BaseStringColumn],
    comparison_value: str,
) -> None:
    """Test string columns `not_like` method."""

    class TestTable(BaseTable):
        qaspen = column()

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
    ("column", "comparison_value"),
    [
        (VarCharColumn, "string"),
        (VarCharColumn, "%string"),
        (VarCharColumn, "%string%"),
        (VarCharColumn, "%string."),
        (TextColumn, "string"),
        (TextColumn, "%string"),
        (TextColumn, "%string%"),
        (TextColumn, "%string."),
    ],
)
def test_str_column_ilike_method_success(
    column: type[BaseStringColumn],
    comparison_value: str,
) -> None:
    """Test string columns `ilike` method."""

    class TestTable(BaseTable):
        qaspen = column()

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
    ("column", "comparison_value"),
    [
        (VarCharColumn, "string"),
        (VarCharColumn, "%string"),
        (VarCharColumn, "%string%"),
        (VarCharColumn, "%string."),
        (TextColumn, "string"),
        (TextColumn, "%string"),
        (TextColumn, "%string%"),
        (TextColumn, "%string."),
    ],
)
def test_str_column_not_ilike_method_success(
    column: type[BaseStringColumn],
    comparison_value: str,
) -> None:
    """Test string columns `not_ilike` method."""

    class TestTable(BaseTable):
        qaspen = column()

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
        (3, "string", ColumnValueValidationError),
    ],
)
def test_varchar_column_validate_column_value_method(
    max_length: int,
    value: str,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `_validate_column_value` method."""

    class TestTable(BaseTable):
        qaspen = VarCharColumn(max_length=max_length)

    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            TestTable.qaspen._validate_column_value(
                column_value=value,
            )
    else:
        TestTable.qaspen._validate_column_value(
            column_value=value,
        )


def test_varchar_column_column_type_property() -> None:
    """Test `_column_type` property."""

    class TestTable(BaseTable):
        qaspen = VarCharColumn(max_length=200)

    assert TestTable.qaspen._column_type == "VARCHAR(200)"


def test_char_column_validate_column_value_method() -> None:
    """Test exception in `_validate_column_value` CharColumn method."""

    class TestTable(BaseTable):
        qaspen = CharColumn()

    with pytest.raises(expected_exception=ColumnValueValidationError):
        TestTable.qaspen._validate_column_value(
            column_value="more_than_one_symbol",
        )


def test_date_column_init_method() -> None:
    """Test `__init__` method.

    Check that it's impossible to declare column
    with `default` and `database_default`.
    """
    with pytest.raises(expected_exception=ColumnDeclarationError):
        DateColumn(
            default=datetime.now().date(),  # noqa: DTZ005
            database_default="CURRENT_DATE",
        )


def test_date_column_column_default_method() -> None:
    """Test `_column_default`.

    Check that return string changes if column has
    `database_default` param.
    """
    column_without_database_default = DateColumn()
    assert column_without_database_default._column_default == ""

    column_with_database_default = DateColumn(database_default="CURRENT_DATE")
    assert (
        column_with_database_default._column_default == "DEFAULT CURRENT_DATE"
    )


def test_timestamp_column_init_method() -> None:
    """Test `__init__` method in `TimestampColumn`.

    Check that it's impossible to declare column
    with `default` and `database_default`.
    """
    with pytest.raises(expected_exception=ColumnDeclarationError):
        TimestampColumn(
            default=datetime.now(),  # noqa: DTZ005
            database_default="CURRENT_DATE",
        )


def test_timestamp_column_column_type_method() -> None:
    """Test `_column_type` in `TimestampColumn`.

    Check that return string changes if column has
    `with_timezone` param.
    """
    column_without_with_timezone = TimestampColumn()
    assert column_without_with_timezone._column_type == "TIMESTAMP"

    column_with_timezone = TimestampTZColumn()
    assert column_with_timezone._column_type == "TIMESTAMP WITH TIME ZONE"


def test_time_column_column_type_method() -> None:
    """Test `_column_type` in `TimestampColumn`.

    Check that return string changes if column has
    `with_timezone` param.
    """
    column_without_with_timezone = TimeColumn()
    assert column_without_with_timezone._column_type == "TIME"

    column_with_timezone = TimeTZColumn()
    assert column_with_timezone._column_type == "TIME WITH TIME ZONE"
