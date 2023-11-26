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
        ("something", "'something'"),
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

    assert (
        filter_instance.querystring().build()
        == f"fortesttable.name = {expected_compare_query}"
    )


@pytest.mark.parametrize(
    ("left_value", "right_value", "expected_compare_query"),
    [
        (
            ForTestTable.name,
            ForTestTable.count,
            "fortesttable.name AND fortesttable.count",
        ),
        (
            ForTestTable.name,
            "something",
            "fortesttable.name AND 'something'",
        ),
        (
            "something",
            "something_else",
            "'something' AND 'something_else'",
        ),
    ],
)
def test_filter_between_querystring_method(
    left_value: Any,
    right_value: Any,
    expected_compare_query: Any,
) -> None:
    """Test `FilterBetween` `querystring` method."""
    filter_instance = FilterBetween(
        field=ForTestTable.name,
        operator=BetweenOperator,
        left_comparison_value=left_value,
        right_comparison_value=right_value,
    )

    assert (
        filter_instance.querystring().build()
        == f"fortesttable.name BETWEEN {expected_compare_query}"
    )


def test_filter_exclusive_querystring_method() -> None:
    """Test `FilterExclusive` `querystring` method."""
    filter_between_instance = FilterBetween(
        field=ForTestTable.name,
        operator=BetweenOperator,
        left_comparison_value="test",
        right_comparison_value="s_test",
    )

    filter_instance = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="default_filter",
    )

    final_filter = FilterExclusive(
        filter_between_instance & filter_instance,
    )

    assert (
        final_filter.querystring().build()
        == "(fortesttable.name BETWEEN 'test' AND 's_test' AND fortesttable.name = 'default_filter')"  # noqa: E501
    )


def test_filter_statement() -> None:
    """Test `FilterStatement` statement."""
    filter_stmt = FilterStatement()

    filter_instance = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="default_filter",
    )
    filter_between_instance = FilterBetween(
        field=ForTestTable.name,
        operator=BetweenOperator,
        left_comparison_value="test",
        right_comparison_value="s_test",
    )

    filter_stmt.add_filter(filter_between_instance)
    filter_stmt.add_filter(filter_instance)

    assert (
        filter_stmt.querystring().build()
        == "WHERE fortesttable.name BETWEEN 'test' AND 's_test' AND fortesttable.name = 'default_filter'"  # noqa: E501
    )


def test_filter_statement_empty() -> None:
    """Test empty `FilterStatement` statement."""
    filter_stmt = FilterStatement()

    assert isinstance(filter_stmt.querystring(), EmptyQueryString)
