from __future__ import annotations

from qaspen.statements.sub_statements.group_by_statement import (
    GroupByStatement,
)
from tests.test_statements.conftest import ForTestTable


def test_group_by_statement_group_by_method() -> None:
    """Test `group_by` method."""
    group_by_stmt = GroupByStatement()
    group_by_stmt.group_by(
        ForTestTable.name,
    )

    assert group_by_stmt.group_bys == [ForTestTable.name]

    group_by_stmt.group_by(
        ForTestTable.count,
    )

    assert group_by_stmt.group_bys == [ForTestTable.name, ForTestTable.count]


def test_group_by_statement_querystring_method() -> None:
    """Test `querystring` method."""
    group_by_stmt = GroupByStatement()

    querystring, qs_params = group_by_stmt.querystring().build()
    assert not querystring
    assert not qs_params

    group_by_stmt.group_by(
        ForTestTable.name,
        ForTestTable.count,
    )

    querystring, qs_params = group_by_stmt.querystring().build()

    assert querystring == "GROUP BY fortesttable.name, fortesttable.count"
    assert not qs_params
