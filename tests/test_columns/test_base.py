"""Tests for BaseColumn."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Final

import pytest

from qaspen.base.operators import All_, Any_
from qaspen.columns.base import Column
from qaspen.columns.operators import (
    AnyOperator,
    BetweenOperator,
    EqualOperator,
    GreaterEqualOperator,
    GreaterOperator,
    InOperator,
    IsNotNullOperator,
    IsNullOperator,
    LessEqualOperator,
    LessOperator,
    NotAnyOperator,
    NotEqualOperator,
    NotInOperator,
)
from qaspen.columns.primitive import VarCharColumn
from qaspen.exceptions import (
    ColumnDeclarationError,
    ColumnValueValidationError,
    FilterComparisonError,
)
from qaspen.qaspen_types import EMPTY_FIELD_VALUE, EMPTY_VALUE, OperatorTypes
from qaspen.sql_type.primitive_types import VarChar
from qaspen.table.base_table import BaseTable
from tests.test_columns.conftest import (
    ForTestColumn,
    ForTestColumnInt,
    _ForTestTable,
    calculate_default_column_value,
)

if TYPE_CHECKING:
    from qaspen.clauses.filter import Filter, FilterBetween


def test_set_name_magic_method() -> None:
    """Test `__set_name__` method.

    Check that column get name from its variable.
    """

    class TestTable(BaseTable):
        wow_column = Column[str]()

    assert TestTable.wow_column._original_column_name == "wow_column"


def test_not_is_primary() -> None:
    """Test negative is_primary and unique attributes."""

    class TestTable(BaseTable):
        wow_column = Column[str]()

    assert not TestTable.wow_column._column_data.is_primary


def test_is_primary() -> None:
    """Test is_primary and unique attributes."""

    class TestTable(BaseTable):
        wow_column = Column[str](is_primary=True)

    assert TestTable.wow_column._column_data.is_primary


def test_not_unique() -> None:
    """Test negative is_primary and unique attributes."""

    class TestTable(BaseTable):
        wow_column = Column[str]()

    assert not TestTable.wow_column._column_data.unique


def test_unique() -> None:
    """Test is_primary and unique attributes."""

    class TestTable(BaseTable):
        wow_column = Column[str](is_primary=True)

    assert TestTable.wow_column._column_data.is_primary


def test_column_value_property(for_test_table: _ForTestTable) -> None:
    """Test `value` property."""
    ttable: Final = for_test_table(  # type: ignore[operator]
        name="123",
        count=1,
    )
    assert ttable.name == "123"


def test_table_name_property() -> None:
    """Test `_table_name` property."""

    class TestTable(BaseTable, table_name="tname"):
        wow_column = Column[str]()

    assert TestTable.wow_column._table_name == "tname"


def test_schemed_table_name_property() -> None:
    """Test `_schemed_table_name` property."""

    class TestTable(BaseTable, table_name="tname"):
        wow_column = Column[str]()

    assert TestTable.wow_column._schemed_table_name == "public.tname"


def test_column_column_name_property() -> None:
    """Test `column_name` property."""

    class TestTable(BaseTable, table_name="tname"):
        wow_column = Column[str]()

    assert TestTable.wow_column.column_name == "tname.wow_column"

    aliased_table = TestTable.aliased(alias="wow_table")

    assert aliased_table.wow_column.column_name == "wow_table.wow_column"


def test_column_column_null_property() -> None:
    """Test `_column_null` property."""

    class TestTable(BaseTable, table_name="tname"):
        wow_column = VarCharColumn()

    assert TestTable.wow_column._column_null == ""

    class TestTable2(BaseTable, table_name="tname"):
        second_column = VarCharColumn(
            is_null=False,
            default="100",
        )

    assert TestTable2.second_column._column_null == "NOT NULL"


def test_column_column_default_property() -> None:
    """Test `_column_default` property."""

    class ForTestTable(_ForTestTable):
        """Class for test purposes."""

        name: ForTestColumn = ForTestColumn(is_null=True)
        count: ForTestColumnInt = ForTestColumnInt(
            default=100,
        )

    assert not ForTestTable.name._column_default
    assert ForTestTable.count._column_default == "DEFAULT 100"


def test_column_column_type_property() -> None:
    """Test `_column_type` property."""

    class TestColumn(Column[str]):
        _sql_type = VarChar

    class TestTable(BaseTable, table_name="tname"):
        wow_column = TestColumn()

    assert TestTable.wow_column._column_type == "VARCHAR"
    type_qs, _ = VarChar.querystring().build()
    assert TestTable.wow_column._column_type == type_qs


def test_no_args_in_parameters() -> None:
    """Test that it's impossible to pass not keyword parameters into Column."""
    with pytest.raises(expected_exception=ColumnDeclarationError):
        Column("some_arg_argument", "second_arg_argument")


def test_db_column_name_param() -> None:
    """Test that db_column_name sets column name."""
    column_name: Final = "cool_column_name"
    column = Column[str](db_column_name=column_name)

    assert column._original_column_name == column_name


def test_automate_column_name() -> None:
    """Test that column_name will be set automatically in Table."""

    class ForTestTable(BaseTable):
        column_in_table = Column[str]()

    assert (
        ForTestTable.column_in_table._original_column_name == "column_in_table"
    )


def test_default_and_database_default() -> None:
    """Check that it's impossible to specify default and database_default."""
    with pytest.raises(expected_exception=ColumnDeclarationError):

        class ForTestTable(BaseTable):
            column_in_table = Column[str](
                default="123",
                database_default="100",
            )


def test_create_table_object_with_none() -> None:
    """Test table creation failure.

    Check if we specify is_null=False and
    try to create table instance without passing
    value to the column, creation will fail.
    """

    class ForTestTable(BaseTable):
        column_in_table = VarCharColumn(is_null=False)

    with pytest.raises(expected_exception=ColumnValueValidationError):
        ForTestTable()

    ForTestTable(column_in_table="123")


@pytest.mark.parametrize(
    ("set_value", "expected_set_value", "expected_exception"),
    [
        (EMPTY_FIELD_VALUE, EMPTY_FIELD_VALUE, None),
        (None, None, None),
        ("correct_string", "correct_string", None),
        (
            {"not": "correct", "type": 2},
            None,
            ColumnValueValidationError,
        ),
    ],
)
def test_column_set_method(
    for_test_table: _ForTestTable,
    set_value: Any,
    expected_set_value: Any,
    expected_exception: type[Exception] | None,
) -> None:
    """Test `__set__` method."""
    table = for_test_table()  # type: ignore[operator]
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            table.name = set_value
    else:
        table.name = set_value

    assert table.name in [expected_set_value]


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
    """Test failure validation of default value for the Column.

    Check that we cannot type for default value that not in
    `_set_available_types`.
    `ForTestColumn` - testing Column that allows only `str` and `float`
    types for `default`.
    """
    with pytest.raises(expected_exception=ColumnValueValidationError):
        ForTestColumn(default=default_value)


def test_default_value_validation_success() -> None:
    """Test success validation of default value for the Column.

    Check that we can set `str`, `float` and `callable`
    default types to ForTestColumn.
    """
    column_with_str = ForTestColumn(default="test_string")
    assert column_with_str._default == "test_string"
    assert column_with_str._prepared_default == "'test_string'"
    column_with_float = ForTestColumn(default=12.0)
    assert column_with_float._default == 12.0  # noqa: PLR2004
    assert column_with_float._prepared_default == "12.0"

    column_with_callable = ForTestColumn(
        default=calculate_default_column_value,
    )
    assert not column_with_callable._default
    assert (
        column_with_callable._callable_default
        == calculate_default_column_value
    )


@pytest.mark.parametrize(
    (
        "is_null",
        "default",
        "db_column_name",
        "expected_default_value",
        "excepted_default_callable_value",
    ),
    [
        (True, None, "wow_name", None, None),
        (False, None, "wow_name", None, None),
        (False, "string", "wow_name", "string", None),
        (False, 12.0, "wow_name", 12.0, None),
        (
            False,
            calculate_default_column_value,
            "wow_name",
            None,
            calculate_default_column_value,
        ),
    ],
)
def test_column_column_data(
    is_null: bool,
    default: str | float | Callable[[], str | float] | None,
    db_column_name: str,
    expected_default_value: str | None,
    excepted_default_callable_value: Callable[[], str | float] | None,
) -> None:
    """Test full main information about the column."""
    column = ForTestColumn(
        is_null=is_null,
        default=default,
        db_column_name=db_column_name,
    )

    assert column._is_null == is_null
    assert column._default == expected_default_value
    assert (
        column._column_data.callable_default == excepted_default_callable_value
    )
    assert column._column_data.column_name == db_column_name


def test_column_in_method_with_values(for_test_table: _ForTestTable) -> None:
    """Test `in_` method with comparison_values.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    comparison_values = (
        "search",
        "this",
        "string",
    )
    filter_statement: Final[Filter] = for_test_table.name.in_(
        *comparison_values,
    )

    assert filter_statement.comparison_values == list(comparison_values)
    assert filter_statement.comparison_value == EMPTY_VALUE
    assert filter_statement.left_operand in [for_test_table.name]
    assert filter_statement.operator == AnyOperator

    querystring, qs_params = filter_statement.querystring().build()
    assert querystring == ("fortesttable.name = ANY(%s)")
    assert qs_params == [list(comparison_values)]


def test_column_in_method_with_subquery(for_test_table: _ForTestTable) -> None:
    """Test `in_` method with subquery.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    subquery = for_test_table.select(for_test_table.name)
    filter_statement: Final[Filter] = for_test_table.name.in_(
        subquery=subquery,
    )

    assert filter_statement.comparison_value == subquery
    assert filter_statement.comparison_values == EMPTY_VALUE
    assert filter_statement.left_operand in [for_test_table.name]
    assert filter_statement.operator == InOperator

    querystring, qs_params = filter_statement.querystring().build()
    assert querystring == (
        "fortesttable.name IN "
        "(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_in_method_with_subquery_and_values(
    for_test_table: _ForTestTable,
) -> None:
    """Test `in_` method.

    Check that it is raising an error if there are both
    subquery and values.
    """
    subquery = for_test_table.select(for_test_table.name)
    with pytest.raises(expected_exception=FilterComparisonError):
        for_test_table.name.in_(
            "search",
            "this",
            "string",
            subquery=subquery,
        )


def test_column_not_in_method_with_values(
    for_test_table: _ForTestTable,
) -> None:
    """Test `not_in` column method.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    comparison_values = (
        "search",
        "this",
        "string",
    )
    filter_statement: Final[Filter] = for_test_table.name.not_in(
        *comparison_values,
    )
    assert filter_statement.comparison_values == list(comparison_values)
    assert filter_statement.comparison_value == EMPTY_VALUE
    assert filter_statement.left_operand in [for_test_table.name]
    assert filter_statement.operator == NotAnyOperator

    querystring, qs_params = filter_statement.querystring().build()
    assert querystring == ("NOT (fortesttable.name = ANY(%s))")
    assert qs_params == [list(comparison_values)]


def test_column_not_in_method_with_subquery(
    for_test_table: _ForTestTable,
) -> None:
    """Test `not_in` method with subquery.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    subquery = for_test_table.select(for_test_table.name)
    filter_statement: Final[Filter] = for_test_table.name.not_in(
        subquery=subquery,
    )

    assert filter_statement.comparison_value == subquery
    assert filter_statement.comparison_values == EMPTY_VALUE
    assert filter_statement.left_operand in [for_test_table.name]
    assert filter_statement.operator == NotInOperator

    querystring, qs_params = filter_statement.querystring().build()
    assert querystring == (
        "fortesttable.name NOT IN "
        "(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_not_in_method_with_subquery_and_values(
    for_test_table: _ForTestTable,
) -> None:
    """Test `not_in` method.

    Check that it is raising an error if there are both
    subquery and values.
    """
    subquery = for_test_table.select(for_test_table.name)
    with pytest.raises(expected_exception=FilterComparisonError):
        for_test_table.name.not_in(
            "search",
            "this",
            "string",
            subquery=subquery,
        )


def test_column_between_method(for_test_table: _ForTestTable) -> None:
    """Test `between` method."""
    left_value: Final = "left"
    right_value: Final = "right"
    filter_between: Final[FilterBetween] = for_test_table.name.between(
        left_value=left_value,
        right_value=right_value,
    )

    assert filter_between.left_comparison_value == left_value
    assert filter_between.right_comparison_value == right_value
    assert filter_between.column in [for_test_table.name]
    assert filter_between.operator == BetweenOperator

    querystring, qs_params = filter_between.querystring().build()
    assert querystring == ("fortesttable.name BETWEEN %s AND %s")
    assert qs_params == [
        left_value,
        right_value,
    ]


def test_column_overloaded_eq_method_with_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that method works correct with column as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_column: Final[Filter] = (
        for_test_table.name == for_test_table.name
    )

    assert filter_with_column.left_operand in [for_test_table.name]
    assert filter_with_column.comparison_value == for_test_table.name
    assert filter_with_column.operator == EqualOperator

    querystring, qs_params = filter_with_column.querystring().build()
    assert querystring == "fortesttable.name = fortesttable.name"
    assert not qs_params


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (Any_, "ANY"),
        (All_, "ALL"),
    ],
)
def test_column_overloaded_eq_method_with_operator(
    for_test_table: _ForTestTable,
    operator_class: OperatorTypes,
    operator_string: str,
) -> None:
    """Test `__eq__` method.

    Check that method works correct with operator as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    operator: OperatorTypes = operator_class(  # type: ignore[operator]
        subquery=for_test_table.select(for_test_table.name),
    )
    filter_with_operator: Final[Filter] = for_test_table.name == operator

    assert filter_with_operator.left_operand in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == EqualOperator

    querystring, qs_params = filter_with_operator.querystring().build()
    assert querystring == (
        "fortesttable.name = "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_overloaded_eq_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name == value

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == EqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name = %s"
    assert qs_params == [value]


def test_column_eq_method_with_none_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that it works normally with None value.
    """
    filter_with_value: Final[Filter] = (
        for_test_table.name == None  # noqa: E711
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == EMPTY_VALUE
    assert filter_with_value.operator == IsNullOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name IS NULL"
    assert not qs_params


def test_column_eq_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `eq` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.eq(
        comparison=value,
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == EqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name = %s"
    assert qs_params == [value]


def test_overloaded_ne_method_with_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ne__` method.

    Check that method works correct with column as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_column: Final[Filter] = (
        for_test_table.name != for_test_table.name
    )

    assert filter_with_column.left_operand in [for_test_table.name]
    assert filter_with_column.comparison_value == for_test_table.name
    assert filter_with_column.operator == NotEqualOperator

    querystring, qs_params = filter_with_column.querystring().build()
    assert querystring == "fortesttable.name != fortesttable.name"
    assert not qs_params


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (Any_, "ANY"),
        (All_, "ALL"),
    ],
)
def test_column_overloaded_ne_method_with_operator(
    for_test_table: _ForTestTable,
    operator_class: OperatorTypes,
    operator_string: str,
) -> None:
    """Test `__ne__` method.

    Check that method works correct with operator as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    operator: OperatorTypes = operator_class(  # type: ignore[operator]
        subquery=for_test_table.select(for_test_table.name),
    )
    filter_with_operator: Final[Filter] = for_test_table.name != operator

    assert filter_with_operator.left_operand in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == NotEqualOperator

    querystring, qs_params = filter_with_operator.querystring().build()
    assert querystring == (
        "fortesttable.name != "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_overloaded_ne_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ne__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name != value

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == NotEqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name != %s"
    assert qs_params == [value]


def test_column_ne_method_with_none_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that it works normally with None value.
    """
    filter_with_value: Final[Filter] = (
        for_test_table.name != None  # noqa: E711
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == EMPTY_VALUE
    assert filter_with_value.operator == IsNotNullOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name IS NOT NULL"
    assert not qs_params


def test_column_ne_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `ne` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.neq(
        comparison_value=value,
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == NotEqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name != %s"
    assert qs_params == [value]


def test_overloaded_gt_method_with_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__gt__` method.

    Check that method works correct with column as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_column: Final[Filter] = (
        for_test_table.name > for_test_table.name
    )

    assert filter_with_column.left_operand in [for_test_table.name]
    assert filter_with_column.comparison_value == for_test_table.name
    assert filter_with_column.operator == GreaterOperator

    querystring, qs_params = filter_with_column.querystring().build()
    assert querystring == "fortesttable.name > fortesttable.name"
    assert not qs_params


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (Any_, "ANY"),
        (All_, "ALL"),
    ],
)
def test_column_overloaded_gt_method_with_operator(
    for_test_table: _ForTestTable,
    operator_class: OperatorTypes,
    operator_string: str,
) -> None:
    """Test `__gt__` method.

    Check that method works correct with operator as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    operator: OperatorTypes = operator_class(  # type: ignore[operator]
        subquery=for_test_table.select(for_test_table.name),
    )
    filter_with_operator: Final[Filter] = for_test_table.name > operator

    assert filter_with_operator.left_operand in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == GreaterOperator

    querystring, qs_params = filter_with_operator.querystring().build()
    assert querystring == (
        "fortesttable.name > "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_overloaded_gt_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__gt__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name > value

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name > %s"
    assert qs_params == [value]


def test_column_gt_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `gt` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.gt(
        comparison_value=value,
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name > %s"
    assert qs_params == [value]


def test_overloaded_ge_method_with_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ge__` method.

    Check that method works correct with column as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_column: Final[Filter] = (
        for_test_table.name >= for_test_table.name
    )

    assert filter_with_column.left_operand in [for_test_table.name]
    assert filter_with_column.comparison_value == for_test_table.name
    assert filter_with_column.operator == GreaterEqualOperator

    querystring, qs_params = filter_with_column.querystring().build()
    assert querystring == "fortesttable.name >= fortesttable.name"
    assert not qs_params


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (Any_, "ANY"),
        (All_, "ALL"),
    ],
)
def test_column_overloaded_ge_method_with_operator(
    for_test_table: _ForTestTable,
    operator_class: OperatorTypes,
    operator_string: str,
) -> None:
    """Test `__ge__` method.

    Check that method works correct with operator as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    operator: OperatorTypes = operator_class(  # type: ignore[operator]
        subquery=for_test_table.select(for_test_table.name),
    )
    filter_with_operator: Final[Filter] = for_test_table.name >= operator

    assert filter_with_operator.left_operand in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == GreaterEqualOperator

    querystring, qs_params = filter_with_operator.querystring().build()
    assert querystring == (
        "fortesttable.name >= "
        f"{operator_string} "
        "(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_overloaded_ge_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ge__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name >= value

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterEqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name >= %s"
    assert qs_params == [value]


def test_column_gte_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `gte` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.gte(
        comparison_value=value,
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterEqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name >= %s"
    assert qs_params == [value]


def test_overloaded_lt_method_with_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__lt__` method.

    Check that method works correct with column as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_column: Final[Filter] = (
        for_test_table.name < for_test_table.name
    )

    assert filter_with_column.left_operand in [for_test_table.name]
    assert filter_with_column.comparison_value == for_test_table.name
    assert filter_with_column.operator == LessOperator

    querystring, qs_params = filter_with_column.querystring().build()
    assert querystring == "fortesttable.name < fortesttable.name"
    assert not qs_params


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (Any_, "ANY"),
        (All_, "ALL"),
    ],
)
def test_column_overloaded_lt_method_with_operator(
    for_test_table: _ForTestTable,
    operator_class: OperatorTypes,
    operator_string: str,
) -> None:
    """Test `__lt__` method.

    Check that method works correct with operator as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    operator: OperatorTypes = operator_class(  # type: ignore[operator]
        subquery=for_test_table.select(for_test_table.name),
    )
    filter_with_operator: Final[Filter] = for_test_table.name < operator

    assert filter_with_operator.left_operand in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == LessOperator

    querystring, qs_params = filter_with_operator.querystring().build()
    assert querystring == (
        "fortesttable.name < "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_overloaded_lt_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__lt__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name < value

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name < %s"
    assert qs_params == [value]


def test_column_lt_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `lt` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.lt(
        comparison_value=value,
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name < %s"
    assert qs_params == [value]


def test_overloaded_le_method_with_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__le__` method.

    Check that method works correct with column as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_column: Final[Filter] = (
        for_test_table.name <= for_test_table.name
    )

    assert filter_with_column.left_operand in [for_test_table.name]
    assert filter_with_column.comparison_value == for_test_table.name
    assert filter_with_column.operator == LessEqualOperator

    querystring, qs_params = filter_with_column.querystring().build()
    assert querystring == "fortesttable.name <= fortesttable.name"
    assert not qs_params


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (Any_, "ANY"),
        (All_, "ALL"),
    ],
)
def test_column_overloaded_le_method_with_operator(
    for_test_table: _ForTestTable,
    operator_class: OperatorTypes,
    operator_string: str,
) -> None:
    """Test `__le__` method.

    Check that method works correct with operator as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    operator: OperatorTypes = operator_class(  # type: ignore[operator]
        subquery=for_test_table.select(for_test_table.name),
    )
    filter_with_operator: Final[Filter] = for_test_table.name <= operator

    assert filter_with_operator.left_operand in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == LessEqualOperator

    querystring, qs_params = filter_with_operator.querystring().build()
    assert querystring == (
        "fortesttable.name <= "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )
    assert not qs_params


def test_column_overloaded_le_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__le__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name <= value

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessEqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name <= %s"
    assert qs_params == [value]


def test_column_lte_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `lte` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.lte(
        comparison_value=value,
    )

    assert filter_with_value.left_operand in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessEqualOperator

    querystring, qs_params = filter_with_value.querystring().build()
    assert querystring == "fortesttable.name <= %s"
    assert qs_params == [value]


def test_column_with_alias_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `with_alias` method.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    alias_name: Final = "good_alias"
    aliased_column: Final = for_test_table.name.with_alias(
        alias_name="good_alias",
    )

    assert aliased_column.alias == alias_name

    statement_with_aliased = for_test_table.select(
        for_test_table.name.with_alias(alias_name=alias_name),
    )

    querystring, qs_params = statement_with_aliased.querystring().build()

    assert querystring == (
        "SELECT fortesttable.name AS good_alias FROM public.fortesttable"
    )
    assert not qs_params


def test_column_correct_method_value_types(
    for_test_table: _ForTestTable,
) -> None:
    """Test that `_correct_method_value_types` returns correct types.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    expected_types = (
        *for_test_table.name._available_comparison_types,
        Column,
        OperatorTypes.__args__,  # type: ignore[attr-defined]
    )

    assert for_test_table.name._correct_method_value_types == expected_types


def test_column_is_the_same_column_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test method `_is_the_same_column`.

    Check that it returns True if columns are the same.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    assert for_test_table.name == for_test_table.name

    assert for_test_table.count != for_test_table.name


def test_column_with_prefix_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test method `_with_prefix`.

    Check that column have prefix after method use.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    prefix_name: Final = "good_prefix"
    prefixed_column = for_test_table.name._with_prefix(
        prefix=prefix_name,
    )

    assert prefixed_column._column_data.prefix == prefix_name


@pytest.mark.parametrize(
    ("value", "excepted_exception"),
    [
        (None, None),
        (calculate_default_column_value, None),
        (12, ColumnValueValidationError),
        ({"not": "correct"}, ColumnValueValidationError),
    ],
)
def test_column_validate_default_value(
    value: Any,
    excepted_exception: None | type[Exception],
    for_test_table: _ForTestTable,
) -> None:
    """Test method `_validate_default_value`.

    Check that method works correctly.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    if excepted_exception:
        with pytest.raises(expected_exception=excepted_exception):
            for_test_table.name._validate_default_value(
                default_value=value,
            )
    else:
        for_test_table.name._validate_default_value(
            default_value=value,
        )


@pytest.mark.parametrize(
    ("value_to_validate", "expected_exception"),
    [
        (None, None),
        ("123", None),
        (12.0, None),
        (["incorrect", "type"], ColumnValueValidationError),
    ],
)
def test_column_validate_column_method(
    value_to_validate: Any,
    expected_exception: type[Exception],
    for_test_table: _ForTestTable,
) -> None:
    """Test `_validate_column_value` `Column` method."""
    if expected_exception:
        with pytest.raises(expected_exception=expected_exception):
            for_test_table.name._validate_column_value(
                column_value=value_to_validate,
            )
    else:
        for_test_table.name._validate_column_value(
            column_value=value_to_validate,
        )


def test_column_is_the_same_column(
    for_test_table: _ForTestTable,
) -> None:
    """Test `_is_the_same_column` `Column` method."""
    assert for_test_table.name._is_the_same_column(
        for_test_table.name,
    )
