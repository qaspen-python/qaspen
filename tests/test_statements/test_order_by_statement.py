from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from qaspen.clauses.order_by import OrderBy
from qaspen.statements.combinable_statements.order_by_statement import (
    OrderByStatement,
)
from tests.test_statements.conftest import ForTestTable

if TYPE_CHECKING:
    from qaspen.fields.base import Field


@pytest.mark.parametrize(
    ("field", "ascending", "nulls_first"),
    [
        (ForTestTable.name, True, True),
        (ForTestTable.name, True, False),
        (ForTestTable.name, False, True),
        (ForTestTable.name, False, False),
    ],
)
def test_order_by_statement_order_by_method(
    field: Field[Any],
    ascending: bool,
    nulls_first: bool,
) -> None:
    """Test `OrderByStatement` `order_by` method.

    Check that it works correctly if pass field.
    """
    order_by_stmt = OrderByStatement()
    order_by_stmt.order_by(
        field=field,
        ascending=ascending,
        nulls_first=nulls_first,
    )

    order_by_expression = order_by_stmt.order_by_expressions[0]
    assert order_by_expression.field in [field]
    assert order_by_expression.ascending == ascending
    assert order_by_expression.nulls_first == nulls_first


@pytest.mark.parametrize(
    "order_by_expression",
    [
        OrderBy(
            field=ForTestTable.name,
            ascending=True,
            nulls_first=True,
        ),
        OrderBy(
            field=ForTestTable.name,
            ascending=True,
            nulls_first=False,
        ),
        OrderBy(
            field=ForTestTable.name,
            ascending=False,
            nulls_first=True,
        ),
        OrderBy(
            field=ForTestTable.name,
            ascending=False,
            nulls_first=False,
        ),
    ],
)
def test_order_by_statement_order_by_method_with_exp(
    order_by_expression: OrderBy,
) -> None:
    """Test `OrderByStatement` `order_by` method.

    Check that it works correctly if `OrderBy`
    expression was passed.
    """
    order_by_stmt = OrderByStatement()
    order_by_stmt.order_by(
        order_by_expressions=[order_by_expression],
    )

    order_by_exp = order_by_stmt.order_by_expressions[0]
    assert order_by_exp == order_by_expression


def test_order_by_statement_querystring_method() -> None:
    """Test `OrderByStatement` `querystring` method."""
    order_by_stmt = OrderByStatement()
    order_by_stmt.order_by(
        order_by_expressions=[
            OrderBy(
                field=ForTestTable.name,
                ascending=True,
                nulls_first=True,
            ),
        ],
    )
    order_by_stmt.order_by(
        field=ForTestTable.name,
    )

    expected_number_of_order_by_expressions = 2
    assert (
        len(order_by_stmt.order_by_expressions)
        == expected_number_of_order_by_expressions
    )

    order_by_exp1 = order_by_stmt.order_by_expressions[0].querystring()
    order_by_exp2 = order_by_stmt.order_by_expressions[0].querystring()
    assert (
        str(order_by_stmt.querystring())
        == f"ORDER BY {order_by_exp1}, {order_by_exp2}"
    )
