from __future__ import annotations

import pytest
from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction

from tests.test_statements.test_select_statement.conftest import UserTable


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` `SelectStatement` method."""
    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    stmt_result = await UserTable.select()
    expected_number_of_results = 2
    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` `SelectStatement` method."""
    test_engine.running_transaction.set(test_db_transaction)

    stmt_result = await UserTable.select().execute(
        engine=test_engine,
    )
    expected_number_of_results = 2
    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `where` `SelectStatement` method."""
    stmt_result = await UserTable.select().transaction_execute(
        transaction=test_db_transaction,
    )
    expected_number_of_results = 2
    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_where_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `where` `SelectStatement` method."""
    stmt = UserTable.select().where(
        UserTable.user_id == 1,
    )

    assert (
        stmt.querystring().build()
        == "SELECT main_users.user_id, main_users.fullname FROM public.main_users WHERE main_users.user_id = 1"  # noqa: E501
    )

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
    ]
    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result
