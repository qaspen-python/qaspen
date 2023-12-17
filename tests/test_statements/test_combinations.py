from __future__ import annotations

from qaspen.clauses.filter import Filter
from qaspen.fields.operators import EqualOperator
from tests.test_statements.conftest import ForTestTable


def test_combinable_expression_and_method() -> None:
    """Test `CombinableExpression` `__and__` method."""
    filter1 = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="123",
    )
    filter2 = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="test_value",
    )

    final_filter = filter1 & filter2

    querystring, qs_params = final_filter.querystring().build()
    assert querystring == ("fortesttable.name = %s AND fortesttable.name = %s")
    assert qs_params == ["123", "test_value"]


def test_combinable_expression_or_method() -> None:
    """Test `CombinableExpression` `__or__` method."""
    filter1 = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="123",
    )
    filter2 = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="test_value",
    )

    final_filter = filter1 | filter2

    querystring, qs_params = final_filter.querystring().build()
    assert querystring == ("fortesttable.name = %s OR fortesttable.name = %s")
    assert qs_params == [
        "123",
        "test_value",
    ]


def test_combinable_expression_invert_method() -> None:
    """Test `CombinableExpression` `__and__` method."""
    filter1 = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="123",
    )

    final_filter = ~filter1

    querystring, qs_params = final_filter.querystring().build()
    assert querystring == ("NOT (fortesttable.name = %s)")
    assert qs_params == [
        "123",
    ]
