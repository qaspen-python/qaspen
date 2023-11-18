"""Tests for BaseField."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Final

import pytest

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
    FilterComparisonError,
)
from qaspen.fields.base import Field
from qaspen.fields.operators import (
    BetweenOperator,
    EqualOperator,
    GreaterEqualOperator,
    GreaterOperator,
    InOperator,
    LessEqualOperator,
    LessOperator,
    NotEqualOperator,
    NotInOperator,
)
from qaspen.qaspen_types import OperatorTypes
from qaspen.statements.combinable_statements.filter_statement import (
    EMPTY_VALUE,
    FilterBetween,
)
from qaspen.table.base_table import BaseTable
from tests.test_fields.base_test.conftest import (
    ForTestField,
    _ForTestTable,
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


def test_field_in_method_with_values(for_test_table: _ForTestTable) -> None:
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

    assert filter_statement.comparison_values == comparison_values
    assert filter_statement.comparison_value == EMPTY_VALUE
    assert filter_statement.field in [for_test_table.name]
    assert filter_statement.operator == InOperator

    querystring = str(filter_statement.querystring())
    assert querystring == ("fortesttable.name IN ('search', 'this', 'string')")


def test_field_in_method_with_subquery(for_test_table: _ForTestTable) -> None:
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
    assert filter_statement.field in [for_test_table.name]
    assert filter_statement.operator == InOperator

    querystring = str(filter_statement.querystring())
    assert querystring == (
        "fortesttable.name IN "
        "(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_in_method_with_subquery_and_values(
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


def test_field_in_method_wrong_parameter_type() -> None:
    """Test `in_` method.

    Check that method is failing if comparison types are wrong.
    """
    with pytest.raises(expected_exception=FieldComparisonError):
        ForTestField().in_(
            "normal_param",
            {"not": "correct", "type": 0},  # type: ignore[arg-type]
        )


def test_field_not_in_method_with_values(
    for_test_table: _ForTestTable,
) -> None:
    """Test `not_in` field method.

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
    assert filter_statement.comparison_values == comparison_values
    assert filter_statement.comparison_value == EMPTY_VALUE
    assert filter_statement.field in [for_test_table.name]
    assert filter_statement.operator == NotInOperator

    querystring = str(filter_statement.querystring())
    assert querystring == (
        "fortesttable.name NOT IN ('search', 'this', 'string')"
    )


def test_field_not_in_method_with_subquery(
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
    assert filter_statement.field in [for_test_table.name]
    assert filter_statement.operator == NotInOperator

    querystring = str(filter_statement.querystring())
    assert querystring == (
        "fortesttable.name NOT IN "
        "(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_not_in_method_with_subquery_and_values(
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


def test_field_not_in_method_wrong_parameter_type() -> None:
    """Test `not_in` method.

    Check that method is failing if comparison types are wrong.
    """
    with pytest.raises(expected_exception=FieldComparisonError):
        ForTestField().in_(
            "normal_param",
            {"not": "correct", "type": 0},  # type: ignore[arg-type]
        )


def test_field_between_method(for_test_table: _ForTestTable) -> None:
    """Test `between` method."""
    left_value: Final = "left"
    right_value: Final = "right"
    filter_between: Final[FilterBetween] = for_test_table.name.between(
        left_value=left_value,
        right_value=right_value,
    )

    assert filter_between.left_comparison_value == left_value
    assert filter_between.right_comparison_value == right_value
    assert filter_between.field in [for_test_table.name]
    assert filter_between.operator == BetweenOperator

    querystring: Final = str(filter_between.querystring())
    assert querystring == ("fortesttable.name BETWEEN 'left' AND 'right'")


@pytest.mark.parametrize(
    ("left_value", "right_value"),
    [
        ("correct", 12),
        (12, "correct"),
        ({"incorrect": 0}, 12),
        ({"incorrect": 0}, [1, 2, 3]),
        ({1, 2, 3}, ("incorrect", "types")),
    ],
)
def test_field_between_method_incorrect_type(
    for_test_table: _ForTestTable,
    left_value: Any,
    right_value: Any,
) -> None:
    """Test `between` method.

    Check that method is failing if comparison types are wrong.
    """
    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name.between(
            left_value=left_value,
            right_value=right_value,
        )


def test_field_overloaded_eq_method_with_field(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that method works correct with field as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_field: Final[Filter] = (
        for_test_table.name == for_test_table.name
    )

    assert filter_with_field.field in [for_test_table.name]
    assert filter_with_field.comparison_value == for_test_table.name
    assert filter_with_field.operator == EqualOperator

    querystring: Final = str(filter_with_field.querystring())
    assert querystring == "fortesttable.name = fortesttable.name"


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (AnyOperator, "ANY"),
        (AllOperator, "ALL"),
    ],
)
def test_field_overloaded_eq_method_with_operator(
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

    assert filter_with_operator.field in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == EqualOperator

    querystring: Final = str(filter_with_operator.querystring())
    assert querystring == (
        "fortesttable.name = "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_overloaded_eq_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name == value

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == EqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name = '{value}'"


def test_field_overloaded_eq_method_wrong_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__eq__` method.

    Check that method fails if comparison value is wrong.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """

    class WrongCompValue:
        pass

    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name == WrongCompValue()  # noqa: B015


def test_field_eq_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `eq` method.

    Check that method works.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name.eq(
        comparison_value=value,
    )

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == EqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name = '{value}'"


def test_overloaded_ne_method_with_field(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ne__` method.

    Check that method works correct with field as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_field: Final[Filter] = (
        for_test_table.name != for_test_table.name
    )

    assert filter_with_field.field in [for_test_table.name]
    assert filter_with_field.comparison_value == for_test_table.name
    assert filter_with_field.operator == NotEqualOperator

    querystring: Final = str(filter_with_field.querystring())
    assert querystring == "fortesttable.name != fortesttable.name"


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (AnyOperator, "ANY"),
        (AllOperator, "ALL"),
    ],
)
def test_field_overloaded_ne_method_with_operator(
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

    assert filter_with_operator.field in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == NotEqualOperator

    querystring: Final = str(filter_with_operator.querystring())
    assert querystring == (
        "fortesttable.name != "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_overloaded_ne_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ne__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name != value

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == NotEqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name != '{value}'"


def test_field_overloaded_ne_method_wrong_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ne__` method.

    Check that method fails if comparison value is wrong.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """

    class WrongCompValue:
        pass

    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name != WrongCompValue()  # noqa: B015


def test_field_ne_method(
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

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == NotEqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name != '{value}'"


def test_overloaded_gt_method_with_field(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__gt__` method.

    Check that method works correct with field as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_field: Final[Filter] = (
        for_test_table.name > for_test_table.name
    )

    assert filter_with_field.field in [for_test_table.name]
    assert filter_with_field.comparison_value == for_test_table.name
    assert filter_with_field.operator == GreaterOperator

    querystring: Final = str(filter_with_field.querystring())
    assert querystring == "fortesttable.name > fortesttable.name"


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (AnyOperator, "ANY"),
        (AllOperator, "ALL"),
    ],
)
def test_field_overloaded_gt_method_with_operator(
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

    assert filter_with_operator.field in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == GreaterOperator

    querystring: Final = str(filter_with_operator.querystring())
    assert querystring == (
        "fortesttable.name > "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_overloaded_gt_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__gt__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name > value

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name > '{value}'"


def test_field_overloaded_gt_method_wrong_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__gt__` method.

    Check that method fails if comparison value is wrong.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """

    class WrongCompValue:
        pass

    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name > WrongCompValue()  # noqa: B015


def test_field_gt_method(
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

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name > '{value}'"


def test_overloaded_ge_method_with_field(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ge__` method.

    Check that method works correct with field as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_field: Final[Filter] = (
        for_test_table.name >= for_test_table.name
    )

    assert filter_with_field.field in [for_test_table.name]
    assert filter_with_field.comparison_value == for_test_table.name
    assert filter_with_field.operator == GreaterEqualOperator

    querystring: Final = str(filter_with_field.querystring())
    assert querystring == "fortesttable.name >= fortesttable.name"


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (AnyOperator, "ANY"),
        (AllOperator, "ALL"),
    ],
)
def test_field_overloaded_ge_method_with_operator(
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

    assert filter_with_operator.field in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == GreaterEqualOperator

    querystring: Final = str(filter_with_operator.querystring())
    assert querystring == (
        "fortesttable.name >= "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_overloaded_ge_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ge__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name >= value

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterEqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name >= '{value}'"


def test_field_overloaded_ge_method_wrong_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__ge__` method.

    Check that method fails if comparison value is wrong.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """

    class WrongCompValue:
        pass

    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name > WrongCompValue()  # noqa: B015


def test_field_gte_method(
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

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == GreaterEqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name >= '{value}'"


def test_overloaded_lt_method_with_field(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__lt__` method.

    Check that method works correct with field as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_field: Final[Filter] = (
        for_test_table.name < for_test_table.name
    )

    assert filter_with_field.field in [for_test_table.name]
    assert filter_with_field.comparison_value == for_test_table.name
    assert filter_with_field.operator == LessOperator

    querystring: Final = str(filter_with_field.querystring())
    assert querystring == "fortesttable.name < fortesttable.name"


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (AnyOperator, "ANY"),
        (AllOperator, "ALL"),
    ],
)
def test_field_overloaded_lt_method_with_operator(
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

    assert filter_with_operator.field in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == LessOperator

    querystring: Final = str(filter_with_operator.querystring())
    assert querystring == (
        "fortesttable.name < "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_overloaded_lt_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__lt__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name < value

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name < '{value}'"


def test_field_overloaded_lt_method_wrong_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__lt__` method.

    Check that method fails if comparison value is wrong.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """

    class WrongCompValue:
        pass

    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name < WrongCompValue()  # noqa: B015


def test_field_lt_method(
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

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name < '{value}'"


def test_overloaded_le_method_with_field(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__le__` method.

    Check that method works correct with field as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    filter_with_field: Final[Filter] = (
        for_test_table.name <= for_test_table.name
    )

    assert filter_with_field.field in [for_test_table.name]
    assert filter_with_field.comparison_value == for_test_table.name
    assert filter_with_field.operator == LessEqualOperator

    querystring: Final = str(filter_with_field.querystring())
    assert querystring == "fortesttable.name <= fortesttable.name"


@pytest.mark.parametrize(
    ("operator_class", "operator_string"),
    [
        (AnyOperator, "ANY"),
        (AllOperator, "ALL"),
    ],
)
def test_field_overloaded_le_method_with_operator(
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

    assert filter_with_operator.field in [for_test_table.name]
    assert filter_with_operator.comparison_value == operator
    assert filter_with_operator.operator == LessEqualOperator

    querystring: Final = str(filter_with_operator.querystring())
    assert querystring == (
        "fortesttable.name <= "
        f"{operator_string} "
        f"(SELECT fortesttable.name FROM public.fortesttable)"
    )


def test_field_overloaded_le_method_with_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__le__` method.

    Check that method works correct with value as a comparison value.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    value: Final = "valid_value"
    filter_with_value: Final[Filter] = for_test_table.name <= value

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessEqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name <= '{value}'"


def test_field_overloaded_le_method_wrong_value(
    for_test_table: _ForTestTable,
) -> None:
    """Test `__le__` method.

    Check that method fails if comparison value is wrong.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """

    class WrongCompValue:
        pass

    with pytest.raises(expected_exception=FieldComparisonError):
        for_test_table.name <= WrongCompValue()  # noqa: B015


def test_field_lte_method(
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

    assert filter_with_value.field in [for_test_table.name]
    assert filter_with_value.comparison_value == value
    assert filter_with_value.operator == LessEqualOperator

    querystring: Final = str(filter_with_value.querystring())
    assert querystring == f"fortesttable.name <= '{value}'"


def test_field_with_alias_method(
    for_test_table: _ForTestTable,
) -> None:
    """Test `with_alias` method.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    alias_name: Final = "good_alias"
    aliased_field: Final = for_test_table.name.with_alias(
        alias_name="good_alias",
    )

    assert aliased_field.alias == alias_name

    statement_with_aliased = for_test_table.select(
        for_test_table.name.with_alias(alias_name=alias_name),
    )

    querystring: Final = str(statement_with_aliased.querystring())

    assert querystring == (
        "SELECT fortesttable.name AS good_alias FROM public.fortesttable"
    )


def test_field__correct_method_value_types(
    for_test_table: _ForTestTable,
) -> None:
    """Test that `_correct_method_value_types` returns correct types.

    ### Parameters:
    - `test_for_test_table`: table for test purposes.
    """
    expected_types = (
        *for_test_table.name._available_comparison_types,
        Field,
        OperatorTypes.__args__,  # type: ignore[attr-defined]
    )

    assert for_test_table.name._correct_method_value_types == expected_types
