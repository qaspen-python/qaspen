from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.statements.insert_statement import InsertStatement
from tests.test_statements.test_insert_statement.conftest import TableTest

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


def test_insert_stmt_init_method() -> None:
    """Test `InsertStatement` `__init__` method."""
    values_to_insert = ([1000],)
    istmt = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=values_to_insert,
    )

    assert istmt._from_table == TableTest
    assert istmt._fields_to_insert == TableTest.all_fields()
    assert istmt._values_to_insert == values_to_insert
    assert not istmt._returning_field


def test_insert_stmt_returning_method() -> None:
    """Test `InsertStatement` `returning` method."""
    values_to_insert = ([1000],)
    istmt = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=values_to_insert,
    ).returning(TableTest.some_id)

    assert istmt._returning_field == TableTest.some_id


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_stmt_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `__await__` method."""
    values_to_insert = ([1000],)
    istmt = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=values_to_insert,
    )

    TableTest._table_meta.database_engine = None
    with pytest.raises(expected_exception=AttributeError):
        await istmt

    test_engine.running_transaction.set(test_db_transaction)
    TableTest._table_meta.database_engine = test_engine

    await istmt
    db_raw_records = await TableTest.select()
    db_results = db_raw_records.result()

    assert len(db_results) == 1

    assert db_results[0]["some_id"] == 1000
    assert db_results[0]["some_name"] == "Qaspen"
    assert db_results[0]["some_number"] == 100

    istmt2 = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=([1000],),
    ).returning(TableTest.some_id)

    result = await istmt2
    assert result[0] == 1000


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_stmt_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `execute` method."""
    values_to_insert = ([1000],)
    istmt = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=values_to_insert,
    )

    TableTest._table_meta.database_engine = None
    test_engine.running_transaction.set(test_db_transaction)

    await istmt.execute(engine=test_engine)
    db_raw_records = await TableTest.select().execute(
        engine=test_engine,
    )
    db_results = db_raw_records.result()

    assert len(db_results) == 1

    assert db_results[0]["some_id"] == 1000
    assert db_results[0]["some_name"] == "Qaspen"
    assert db_results[0]["some_number"] == 100

    istmt2 = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=([1000],),
    ).returning(TableTest.some_id)

    result = await istmt2.execute(engine=test_engine)
    assert result[0] == 1000


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_stmt_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `transaction_execute` method."""
    values_to_insert = ([1000],)
    istmt = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=values_to_insert,
    )

    TableTest._table_meta.database_engine = None

    await istmt.transaction_execute(
        transaction=test_db_transaction,
    )
    db_raw_records = await TableTest.select().transaction_execute(
        transaction=test_db_transaction,
    )
    db_results = db_raw_records.result()

    assert len(db_results) == 1

    assert db_results[0]["some_id"] == 1000
    assert db_results[0]["some_name"] == "Qaspen"
    assert db_results[0]["some_number"] == 100

    istmt2 = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[TableTest.some_id],
        values_to_insert=([1000],),
    ).returning(TableTest.some_id)

    result = await istmt2.transaction_execute(
        transaction=test_db_transaction,
    )
    assert result[0] == 1000


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_stmt_all_fields(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement`.

    Specify all fields.
    """
    values_to_insert = ([999, "qaspen_wow", 10],)
    istmt = InsertStatement[TableTest, None](
        from_table=TableTest,
        fields_to_insert=[
            TableTest.some_id,
            TableTest.some_name,
            TableTest.some_number,
        ],
        values_to_insert=values_to_insert,
    )

    await istmt.transaction_execute(
        transaction=test_db_transaction,
    )

    db_raw_records = await TableTest.select().transaction_execute(
        transaction=test_db_transaction,
    )
    db_results = db_raw_records.result()

    assert len(db_results) == 1

    assert db_results[0]["some_id"] == 999
    assert db_results[0]["some_name"] == "qaspen_wow"
    assert db_results[0]["some_number"] == 10
