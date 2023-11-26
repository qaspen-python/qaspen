from __future__ import annotations

from typing import Final

from qaspen.statements.sub_statements.limit_statement import LimitStatement


def test_limit_statement() -> None:
    """Test `LimitStatement`."""
    limit_stmt = LimitStatement()
    limit_number: Final = 10
    limit_stmt.limit(limit_number)

    assert limit_stmt.limit_number == limit_number

    new_limit_number: Final = 20
    limit_stmt.limit(new_limit_number)

    assert limit_stmt.limit_number == new_limit_number

    assert limit_stmt.querystring().build() == f"LIMIT {new_limit_number}"
