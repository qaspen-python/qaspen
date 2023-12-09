from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.statements.insert_statement import InsertObjectsStatement
from tests.test_statements.test_insert_statement.conftest import TableTest

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


def test_insert_obj_stmt_init_method() -> None:
    """Test `InsertObjectsStatement` `__init__` method."""
    insert_objects = [TableTest(some_id=123)]
    iostmt = InsertObjectsStatement[TableTest, None](
        insert_objects=insert_objects,
        from_table=TableTest,
    )
    assert iostmt._from_table == TableTest
    assert iostmt._insert_objects == insert_objects
    assert not iostmt._returning_field


def test_insert_obj_stmt_returning_method() -> None:
    """Test `InsertObjectsStatement` `returning` method."""
    insert_objects = [TableTest(some_id=123)]
    iostmt = InsertObjectsStatement[TableTest, None](
        insert_objects=insert_objects,
        from_table=TableTest,
    ).returning(TableTest.some_id)

    assert iostmt._returning_field == TableTest.some_id


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_obj_stmt_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `__await__` method."""
    table_obj1_some_id = 1000
    table_obj1 = TableTest(some_id=table_obj1_some_id)

    table_obj2_some_id = 999
    table_obj2_some_name = "ORM"
    table_obj2 = TableTest(
        some_id=table_obj2_some_id,
        some_name=table_obj2_some_name,
    )

    insert_objects = [
        table_obj1,
        table_obj2,
    ]

    iostmt = InsertObjectsStatement[TableTest, None](
        insert_objects=insert_objects,
        from_table=TableTest,
    )

    TableTest._table_meta.database_engine = None
    with pytest.raises(expected_exception=AttributeError):
        await iostmt

    test_engine.running_transaction.set(test_db_transaction)
    TableTest._table_meta.database_engine = test_engine

    await iostmt
    db_raw_records = await TableTest.select()
    db_results = db_raw_records.result()
    expected_db_results = [
        {"some_id": 1000, "some_name": "Qaspen", "some_number": 100},
        {"some_id": 999, "some_name": "ORM", "some_number": 100},
    ]

    assert db_results == expected_db_results


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_obj_stmt_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `__await__` method."""
    TableTest._table_meta.database_engine = None
    table_obj1_some_id = 1000
    table_obj1 = TableTest(some_id=table_obj1_some_id)

    table_obj2_some_id = 999
    table_obj2_some_name = "ORM"
    table_obj2 = TableTest(
        some_id=table_obj2_some_id,
        some_name=table_obj2_some_name,
    )

    insert_objects = [
        table_obj1,
        table_obj2,
    ]

    iostmt = InsertObjectsStatement[TableTest, None](
        insert_objects=insert_objects,
        from_table=TableTest,
    ).returning(TableTest.some_id)

    test_engine.running_transaction.set(test_db_transaction)

    query_result = await iostmt.execute(engine=test_engine)
    assert query_result == [1000, 999]

    db_raw_records = await TableTest.select().execute(engine=test_engine)
    db_results = db_raw_records.result()
    expected_db_results = [
        {"some_id": 1000, "some_name": "Qaspen", "some_number": 100},
        {"some_id": 999, "some_name": "ORM", "some_number": 100},
    ]

    assert db_results == expected_db_results


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_obj_stmt_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `__await__` method."""
    TableTest._table_meta.database_engine = None
    table_obj1_some_id = 1000
    table_obj1 = TableTest(some_id=table_obj1_some_id)

    table_obj2_some_id = 999
    table_obj2_some_name = "ORM"
    table_obj2 = TableTest(
        some_id=table_obj2_some_id,
        some_name=table_obj2_some_name,
    )

    insert_objects = [
        table_obj1,
        table_obj2,
    ]

    iostmt = InsertObjectsStatement[TableTest, None](
        insert_objects=insert_objects,
        from_table=TableTest,
    ).returning(TableTest.some_id)

    query_result = await iostmt.transaction_execute(
        transaction=test_db_transaction,
    )
    assert query_result == [1000, 999]

    db_raw_records = await TableTest.select().transaction_execute(
        transaction=test_db_transaction,
    )
    db_results = db_raw_records.result()
    expected_db_results = [
        {"some_id": 1000, "some_name": "Qaspen", "some_number": 100},
        {"some_id": 999, "some_name": "ORM", "some_number": 100},
    ]

    assert db_results == expected_db_results


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_table")
async def test_insert_obj_stmt_transaction_execute_method_no_return(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `InsertStatement` `__await__` method."""
    TableTest._table_meta.database_engine = None
    table_obj1_some_id = 1000
    table_obj1 = TableTest(some_id=table_obj1_some_id)

    table_obj2_some_id = 999
    table_obj2_some_name = "ORM"
    table_obj2 = TableTest(
        some_id=table_obj2_some_id,
        some_name=table_obj2_some_name,
    )

    insert_objects = [
        table_obj1,
        table_obj2,
    ]

    iostmt = InsertObjectsStatement[TableTest, None](
        insert_objects=insert_objects,
        from_table=TableTest,
    )

    await iostmt.transaction_execute(
        transaction=test_db_transaction,
    )

    db_raw_records = await TableTest.select().transaction_execute(
        transaction=test_db_transaction,
    )
    db_results = db_raw_records.result()
    expected_db_results = [
        {"some_id": 1000, "some_name": "Qaspen", "some_number": 100},
        {"some_id": 999, "some_name": "ORM", "some_number": 100},
    ]

    assert db_results == expected_db_results
