from __future__ import annotations

from typing import Final

import pytest

from qaspen.querystring.querystring import (
    CommaSeparatedQueryString,
    FilterQueryString,
    FullStatementQueryString,
    QueryString,
)


@pytest.mark.parametrize(
    "querystring",
    [
        QueryString,
        CommaSeparatedQueryString,
        FilterQueryString,
        FullStatementQueryString,
    ],
)
def test_querystring_build(querystring: type[QueryString]) -> None:
    """Test `QueryString` building."""
    qs: Final = querystring(
        "column",
        "table",
        "orm",
        template_parameters=["qaspen"],
        sql_template=(
            f"SELECT {QueryString.arg_ph()} "
            f"FROM {QueryString.arg_ph()} "
            f"WHERE {QueryString.arg_ph()} = "
            f"{QueryString.param_ph()}"
        ),
    )
    built_qs, qs_params = qs.build()
    assert built_qs == "SELECT column FROM table WHERE orm = %s"
    assert qs_params == ["qaspen"]


def test_querystring_add() -> None:
    """Test `QueryString` `__add__` method."""
    qs1 = QueryString(
        "column",
        "table",
        sql_template=(
            f"SELECT {QueryString.arg_ph()} FROM {QueryString.arg_ph()}"
        ),
    )

    assert str(qs1) == qs1.sql_template

    qs2 = QueryString(
        "column",
        "'wow'",
        sql_template=(
            f"WHERE {QueryString.arg_ph()} = {QueryString.arg_ph()}"
        ),
    )

    final_qs = qs1 + qs2
    built_qs, qs_params = final_qs.build()
    assert built_qs == "SELECT column FROM table WHERE column = 'wow'"
    assert not qs_params
