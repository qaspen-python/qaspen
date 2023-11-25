from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from qaspen.aggregate_functions.general_purpose import (
    ArrayAgg,
    Avg,
    Coalesce,
    Count,
    Greatest,
    Least,
    Max,
    Min,
    StringAgg,
    Sum,
)
from qaspen.clauses.order_by import OrderBy
from tests.test_agg_functions.conftest import TableForTest

if TYPE_CHECKING:
    from qaspen.base.sql_base import SQLSelectable


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT COUNT(ttest.name) FROM public.ttest",
        ),
        (
            "something",
            "SELECT COUNT('something') FROM public.ttest",
        ),
    ],
)
def test_count_agg_function(
    func_argument: Any,
    expected_query: str,
) -> None:
    """Test `Count` agg function."""
    agg_statement = TableForTest.select(
        Count(func_argument=func_argument),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_arguments", "expected_query"),
    [
        (
            [TableForTest.name, "something"],
            "SELECT COALESCE(ttest.name, 'something') FROM public.ttest",
        ),
        (
            [TableForTest.name],
            "SELECT COALESCE(ttest.name) FROM public.ttest",
        ),
    ],
)
def test_coalesce_agg_function(
    func_arguments: Any,
    expected_query: str,
) -> None:
    """Test `COALESCE` agg function."""
    agg_statement = TableForTest.select(
        Coalesce(*func_arguments),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT AVG(ttest.name) FROM public.ttest",
        ),
        (
            "something",
            "SELECT AVG('something') FROM public.ttest",
        ),
    ],
)
def test_avg_agg_function(
    func_argument: Any,
    expected_query: str,
) -> None:
    """Test `Avg` agg function."""
    agg_statement = TableForTest.select(
        Avg(func_argument=func_argument),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT ARRAY_AGG(ttest.name) FROM public.ttest",
        ),
        (
            "something",
            "SELECT ARRAY_AGG('something') FROM public.ttest",
        ),
    ],
)
def test_array_agg_function_simple(
    func_argument: Any,
    expected_query: str,
) -> None:
    """Test `ArrayAgg` agg function."""
    agg_statement = TableForTest.select(
        ArrayAgg(func_argument=func_argument),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "order_by", "expected_query"),
    [
        (
            TableForTest.name,
            [TableForTest.name],
            (
                "SELECT ARRAY_AGG(ttest.name ORDER BY ttest.name) "
                "FROM public.ttest"
            ),
        ),
        (
            TableForTest.name,
            [TableForTest.name, TableForTest.count],
            (
                "SELECT ARRAY_AGG"
                "(ttest.name ORDER BY ttest.name, ttest.count) "
                "FROM public.ttest"
            ),
        ),
        (
            "something",
            [TableForTest.name],
            (
                "SELECT ARRAY_AGG('something' ORDER BY ttest.name) "
                "FROM public.ttest"
            ),
        ),
    ],
)
def test_array_agg_function_with_order_by(
    func_argument: Any,
    order_by: list[SQLSelectable],
    expected_query: str,
) -> None:
    """Test `ArrayAgg` agg function with `order_by` parameter."""
    agg_statement = TableForTest.select(
        ArrayAgg(
            func_argument=func_argument,
            order_by=order_by,
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "order_by_objs", "expected_query"),
    [
        (
            TableForTest.name,
            [OrderBy(field=TableForTest.name)],
            (
                "SELECT ARRAY_AGG(ttest.name ORDER BY ttest.name ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
        (
            TableForTest.name,
            [
                OrderBy(
                    field=TableForTest.name,
                ),
                OrderBy(
                    field=TableForTest.count,
                    ascending=False,
                    nulls_first=False,
                ),
            ],
            (
                "SELECT ARRAY_AGG(ttest.name ORDER BY ttest.name ASC NULLS FIRST, ttest.count DESC NULLS LAST) FROM public.ttest"  # noqa: E501
            ),
        ),
        (
            "something",
            [OrderBy(field=TableForTest.name)],
            (
                "SELECT ARRAY_AGG('something' ORDER BY ttest.name ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
    ],
)
def test_array_agg_function_with_order_by_objs(
    func_argument: Any,
    order_by_objs: list[OrderBy],
    expected_query: str,
) -> None:
    """Test `ArrayAgg` agg function with `order_by` parameter."""
    agg_statement = TableForTest.select(
        ArrayAgg(
            func_argument=func_argument,
            order_by_objs=order_by_objs,
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "order_by", "order_by_objs", "expected_query"),
    [
        (
            TableForTest.name,
            [TableForTest.name],
            [OrderBy(field=TableForTest.count)],
            (
                "SELECT ARRAY_AGG(ttest.name ORDER BY ttest.name, ttest.count ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
        (
            "something",
            [TableForTest.name],
            [OrderBy(field=TableForTest.count)],
            (
                "SELECT ARRAY_AGG('something' ORDER BY ttest.name, ttest.count ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
    ],
)
def test_array_agg_function_with_order_by_objs_and_order_by(
    func_argument: Any,
    order_by: list[SQLSelectable],
    order_by_objs: list[OrderBy],
    expected_query: str,
) -> None:
    """Test `ArrayAgg` agg function.

    Check that it works correctly with `order_by` and
    `order_by_objs` parameters.
    """
    agg_statement = TableForTest.select(
        ArrayAgg(
            func_argument=func_argument,
            order_by=order_by,
            order_by_objs=order_by_objs,
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT SUM(ttest.name) FROM public.ttest",
        ),
        (
            TableForTest.count,
            "SELECT SUM(ttest.count) FROM public.ttest",
        ),
    ],
)
def test_sum_agg_function(
    func_argument: SQLSelectable,
    expected_query: str,
) -> None:
    """Test `Count` sum function."""
    agg_statement = TableForTest.select(
        Sum(func_argument=func_argument),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT STRING_AGG(ttest.name, ',') FROM public.ttest",
        ),
        (
            "something",
            "SELECT STRING_AGG('something', ',') FROM public.ttest",
        ),
    ],
)
def test_string_agg_function_simple(
    func_argument: Any,
    expected_query: str,
) -> None:
    """Test `StringAgg` agg function."""
    agg_statement = TableForTest.select(
        StringAgg(
            func_argument=func_argument,
            separator=",",
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "order_by", "expected_query"),
    [
        (
            TableForTest.name,
            [TableForTest.name],
            (
                "SELECT STRING_AGG(ttest.name, ',' ORDER BY ttest.name) "
                "FROM public.ttest"
            ),
        ),
        (
            TableForTest.name,
            [TableForTest.name, TableForTest.count],
            (
                "SELECT STRING_AGG"
                "(ttest.name, ',' ORDER BY ttest.name, ttest.count) "
                "FROM public.ttest"
            ),
        ),
    ],
)
def test_string_agg_function_with_order_by(
    func_argument: Any,
    order_by: list[SQLSelectable],
    expected_query: str,
) -> None:
    """Test `StringAgg` agg function with `order_by` parameter."""
    agg_statement = TableForTest.select(
        StringAgg(
            separator=",",
            func_argument=func_argument,
            order_by=order_by,
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "order_by_objs", "expected_query"),
    [
        (
            TableForTest.name,
            [OrderBy(field=TableForTest.name)],
            (
                "SELECT STRING_AGG(ttest.name, ',' ORDER BY ttest.name ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
        (
            TableForTest.name,
            [
                OrderBy(
                    field=TableForTest.name,
                ),
                OrderBy(
                    field=TableForTest.count,
                    ascending=False,
                    nulls_first=False,
                ),
            ],
            (
                "SELECT STRING_AGG(ttest.name, ',' ORDER BY ttest.name ASC NULLS FIRST, ttest.count DESC NULLS LAST) FROM public.ttest"  # noqa: E501
            ),
        ),
    ],
)
def test_string_agg_function_with_order_by_objs(
    func_argument: Any,
    order_by_objs: list[OrderBy],
    expected_query: str,
) -> None:
    """Test `StringAgg` agg function with `order_by` parameter."""
    agg_statement = TableForTest.select(
        StringAgg(
            separator=",",
            func_argument=func_argument,
            order_by_objs=order_by_objs,
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "order_by", "order_by_objs", "expected_query"),
    [
        (
            TableForTest.name,
            [TableForTest.name],
            [OrderBy(field=TableForTest.count)],
            (
                "SELECT STRING_AGG(ttest.name, ',' ORDER BY ttest.name, ttest.count ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
        (
            TableForTest.count,
            [TableForTest.name],
            [OrderBy(field=TableForTest.count)],
            (
                "SELECT STRING_AGG(ttest.count, ',' ORDER BY ttest.name, ttest.count ASC NULLS FIRST) FROM public.ttest"  # noqa: E501
            ),
        ),
    ],
)
def test_string_agg_function_with_order_by_objs_and_order_by(
    func_argument: Any,
    order_by: list[SQLSelectable],
    order_by_objs: list[OrderBy],
    expected_query: str,
) -> None:
    """Test `StringAgg` agg function.

    Check that it works correctly with `order_by` and
    `order_by_objs` parameters.
    """
    agg_statement = TableForTest.select(
        StringAgg(
            separator=",",
            func_argument=func_argument,
            order_by=order_by,
            order_by_objs=order_by_objs,
        ),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT MAX(ttest.name) FROM public.ttest",
        ),
        (
            TableForTest.count,
            "SELECT MAX(ttest.count) FROM public.ttest",
        ),
    ],
)
def test_max_agg_function(
    func_argument: Any,
    expected_query: str,
) -> None:
    """Test `Max` agg function."""
    agg_statement = TableForTest.select(
        Max(func_argument=func_argument),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_argument", "expected_query"),
    [
        (
            TableForTest.name,
            "SELECT MIN(ttest.name) FROM public.ttest",
        ),
        (
            TableForTest.count,
            "SELECT MIN(ttest.count) FROM public.ttest",
        ),
    ],
)
def test_min_agg_function(
    func_argument: Any,
    expected_query: str,
) -> None:
    """Test `Min` agg function."""
    agg_statement = TableForTest.select(
        Min(func_argument=func_argument),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_arguments", "expected_query"),
    [
        (
            [TableForTest.name],
            "SELECT GREATEST(ttest.name) FROM public.ttest",
        ),
        (
            [TableForTest.name, TableForTest.count],
            "SELECT GREATEST(ttest.name, ttest.count) FROM public.ttest",
        ),
    ],
)
def test_greatest_agg_function(
    func_arguments: Any,
    expected_query: str,
) -> None:
    """Test `Greatest` agg function."""
    agg_statement = TableForTest.select(
        Greatest(*func_arguments),
    )

    assert str(agg_statement.querystring()) == expected_query


@pytest.mark.parametrize(
    ("func_arguments", "expected_query"),
    [
        (
            [TableForTest.name],
            "SELECT LEAST(ttest.name) FROM public.ttest",
        ),
        (
            [TableForTest.name, TableForTest.count],
            "SELECT LEAST(ttest.name, ttest.count) FROM public.ttest",
        ),
    ],
)
def test_least_agg_function(
    func_arguments: Any,
    expected_query: str,
) -> None:
    """Test `Least` agg function."""
    agg_statement = TableForTest.select(
        Least(*func_arguments),
    )

    assert str(agg_statement.querystring()) == expected_query
