from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.columns.primitive import IntegerColumn, VarCharColumn
from qaspen.table.base_table import BaseTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgTransaction


def default_some_number() -> int:
    """Return default number."""
    return 100


class TableTest(BaseTable, table_name="table_test"):
    """Class for testing joins."""

    some_id: IntegerColumn = IntegerColumn()
    some_name: VarCharColumn = VarCharColumn(
        default="Qaspen",
        is_null=False,
    )
    some_number: IntegerColumn = IntegerColumn(
        default=default_some_number,
        is_null=False,
    )


@pytest.fixture()
async def _create_test_table(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Create `TableTest` table in the database."""
    await test_db_transaction.execute(
        fetch_results=False,
        querystring_parameters=[],
        querystring=(
            """
            CREATE TABLE table_test (
                some_id INTEGER,
                some_name VARCHAR,
                some_number INTEGER
            );
            """
        ),
    )
