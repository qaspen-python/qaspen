from __future__ import annotations

from typing import Final

import pytest

from qaspen.querystring.querystring import (
    FilterQueryString,
    FullStatementQueryString,
    OrderByQueryString,
    QueryString,
)


@pytest.mark.parametrize(
    "querystring",
    [
        QueryString,
        OrderByQueryString,
        FilterQueryString,
        FullStatementQueryString,
    ],
)
def test_querystring_build(querystring: type[QueryString]) -> None:
    """Test `QueryString` building."""
    qs: Final = querystring(
        "field",
        "table",
        sql_template="SELECT {} FROM {}",
    )
    assert str(qs) == "SELECT field FROM table"


def test_querystring_add() -> None:
    """Test `QueryString` `__add__` method."""
    qs1 = QueryString(
        "field",
        "table",
        sql_template="SELECT {} FROM {}",
    )

    qs2 = QueryString(
        "field",
        "'wow'",
        sql_template="WHERE {} = {}",
    )

    final_qs = qs1 + qs2
    assert (
        final_qs.querystring()
    ) == "SELECT field FROM table WHERE field = 'wow'"
