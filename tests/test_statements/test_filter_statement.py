from __future__ import annotations

from typing import Any

import pytest

from qaspen.fields.operators import BetweenOperator, EqualOperator
from qaspen.querystring.querystring import EmptyQueryString
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
    FilterExclusive,
    FilterStatement,
)
from tests.test_statements.conftest import ForTestTable


@pytest.mark.parametrize(
    ("comparison_value", "expected_compare_query"),
    [
        (ForTestTable.count, "fortesttable.count"),
        ("something", "something"),
    ],
)
def test_filter_querystring_method(
    comparison_value: Any,
    expected_compare_query: str,
) -> None:
    """Test `Filter` `querystring` method."""
    filter_instance = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value=comparison_value,
    )

    querystring, qs_params = filter_instance.querystring().build()
    assert querystring == "fortesttable.name = %s"
    assert expected_compare_query in qs_params


@pytest.mark.parametrize(
    (
        "left_value",
        "right_value",
        "expected_compare_query",
        "expected_params",
    ),
    [
        (
            ForTestTable.name,
            ForTestTable.count,
            "fortesttable.name BETWEEN %s AND %s",
            ["fortesttable.name", "fortesttable.count"],
        ),
        (
            ForTestTable.name,
            "something",
            "fortesttable.name BETWEEN %s AND %s",
            ["fortesttable.name", "something"],
        ),
        (
            "something",
            "something_else",
            "fortesttable.name BETWEEN %s AND %s",
            ["something", "something_else"],
        ),
    ],
)
def test_filter_between_querystring_method(
    left_value: Any,
    right_value: Any,
    expected_compare_query: Any,
    expected_params: list[str],
) -> None:
    """Test `FilterBetween` `querystring` method."""
    filter_instance = FilterBetween(
        field=ForTestTable.name,
        operator=BetweenOperator,
        left_comparison_value=left_value,
        right_comparison_value=right_value,
    )

    querystring, qs_params = filter_instance.querystring().build()
    assert querystring == expected_compare_query
    assert qs_params == expected_params


def test_filter_exclusive_querystring_method() -> None:
    """Test `FilterExclusive` `querystring` method."""
    left_comparison_value = "test"
    right_comparison_value = "s_test"
    filter_between_instance = FilterBetween(
        field=ForTestTable.name,
        operator=BetweenOperator,
        left_comparison_value=left_comparison_value,
        right_comparison_value=right_comparison_value,
    )

    comparison_value = "default_filter"
    filter_instance = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value=comparison_value,
    )

    final_filter = FilterExclusive(
        filter_between_instance & filter_instance,
    )

    querystring, qs_params = final_filter.querystring().build()
    assert querystring == (
        "(fortesttable.name BETWEEN %s AND %s AND fortesttable.name = %s)"
    )
    assert qs_params == [
        left_comparison_value,
        right_comparison_value,
        comparison_value,
    ]


def test_filter_statement() -> None:
    """Test `FilterStatement` statement."""
    filter_stmt = FilterStatement()

    left_comparison_value = "test"
    right_comparison_value = "s_test"
    filter_between_instance = FilterBetween(
        field=ForTestTable.name,
        operator=BetweenOperator,
        left_comparison_value=left_comparison_value,
        right_comparison_value=right_comparison_value,
    )

    filter_stmt.add_filter(filter_between_instance)

    comparison_value = "default_filter"
    filter_instance = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value=comparison_value,
    )

    filter_stmt.add_filter(filter_instance)

    querystring, qs_params = filter_stmt.querystring().build()
    assert querystring == (
        "WHERE fortesttable.name BETWEEN %s AND %s AND fortesttable.name = %s"
    )
    assert qs_params == [
        left_comparison_value,
        right_comparison_value,
        comparison_value,
    ]


def test_filter_statement_empty() -> None:
    """Test empty `FilterStatement` statement."""
    filter_stmt = FilterStatement()

    assert isinstance(filter_stmt.querystring(), EmptyQueryString)
