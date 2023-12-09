from __future__ import annotations

from typing import Final

from qaspen.statements.sub_statements.offset_statement import OffsetStatement


def test_offset_statement() -> None:
    """Test `OffsetStatement`."""
    offset_stmt = OffsetStatement()
    offset_number: Final = 10
    offset_stmt.offset(offset_number)

    assert offset_stmt.offset_number == offset_number

    new_offset_number: Final = 20
    offset_stmt.offset(new_offset_number)

    assert offset_stmt.offset_number == new_offset_number

    querystring, qs_params = offset_stmt.querystring().build()
    assert querystring == f"OFFSET {new_offset_number}"
    assert not qs_params
