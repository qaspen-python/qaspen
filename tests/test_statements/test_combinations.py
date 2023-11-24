from __future__ import annotations

from qaspen.fields.operators import EqualOperator
from qaspen.statements.combinable_statements.filter_statement import Filter
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
        comparison_value="123",
    )

    final_filter = filter1 & filter2
    assert (
        str(final_filter.querystring())
        == "fortesttable.name = '123' AND fortesttable.name = '123'"
    )


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
        comparison_value="123",
    )

    final_filter = filter1 | filter2
    assert (
        str(final_filter.querystring())
        == "fortesttable.name = '123' OR fortesttable.name = '123'"
    )


def test_combinable_expression_invert_method() -> None:
    """Test `CombinableExpression` `__and__` method."""
    filter1 = Filter(
        field=ForTestTable.name,
        operator=EqualOperator,
        comparison_value="123",
    )

    final_filter = ~filter1
    assert str(final_filter.querystring()) == "NOT (fortesttable.name = '123')"
