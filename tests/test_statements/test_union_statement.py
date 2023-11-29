from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.test_statements.conftest import UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_union_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` `UnionStatement` method."""
    stmt1 = UserTable.select(
        UserTable.fullname,
    )

    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    stmt3 = UserTable.select(
        UserTable.fullname,
    )

    union = stmt1.union(stmt2)
    union = union.union(stmt3)

    UserTable._table_meta.database_engine = None

    with pytest.raises(expected_exception=AttributeError):
        await union

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    assert (
        union.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users UNION SELECT main_users.fullname FROM public.main_users UNION SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )

    expected_result = [
        {"fullname": "Python"},
        {"fullname": "Qaspen"},
    ]
    assert await union == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_union_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` `UnionStatement` method."""
    test_engine.running_transaction.set(test_db_transaction)

    stmt1 = UserTable.select(
        UserTable.fullname,
    )

    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    union = stmt1.union(stmt2, union_all=True)

    assert (
        union.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users UNION ALL SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )

    expected_result = [
        {"fullname": "Qaspen"},
        {"fullname": "Python"},
        {"fullname": "Qaspen"},
        {"fullname": "Python"},
    ]
    assert (
        await union.execute(
            engine=test_engine,
        )
        == expected_result
    )


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_union_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `transaction_execute` `UnionStatement` method."""
    stmt1 = UserTable.select(
        UserTable.fullname,
    )

    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    stmt3 = UserTable.select(
        UserTable.fullname,
    )

    union = stmt1.union(stmt2)
    union = union.union(stmt3)

    assert (
        union.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users UNION SELECT main_users.fullname FROM public.main_users UNION SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )

    expected_result = [{"fullname": "Python"}, {"fullname": "Qaspen"}]
    assert (
        await union.transaction_execute(
            transaction=test_db_transaction,
        )
        == expected_result
    )
