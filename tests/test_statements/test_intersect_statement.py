from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.test_statements.conftest import UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_intersect_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` `IntersectStatement` method."""
    stmt1 = UserTable.select(
        UserTable.fullname,
    )

    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    stmt3 = UserTable.select(
        UserTable.fullname,
    )

    UserTable._table_meta.database_engine = None
    intersect = stmt1.intersect(stmt2)
    intersect = intersect.intersect(stmt3)

    with pytest.raises(expected_exception=AttributeError):
        await intersect

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    assert (
        intersect.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users INTERSECT SELECT main_users.fullname FROM public.main_users INTERSECT SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )

    expected_result = [{"fullname": "Python"}, {"fullname": "Qaspen"}]
    assert await intersect == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_intersect_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` `IntersectStatement` method."""
    test_engine.running_transaction.set(test_db_transaction)

    stmt1 = UserTable.select(
        UserTable.fullname,
    )

    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    intersect = stmt1.intersect(stmt2)

    assert (
        intersect.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users INTERSECT SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )

    expected_result = [{"fullname": "Python"}, {"fullname": "Qaspen"}]
    assert (
        await intersect.execute(
            engine=test_engine,
        )
        == expected_result
    )


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_intersect_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `transaction_execute` `IntersectStatement` method."""
    stmt1 = UserTable.select(
        UserTable.fullname,
    )

    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    stmt3 = UserTable.select(
        UserTable.fullname,
    )

    intersect = stmt1.intersect(stmt2)
    intersect = intersect.intersect(stmt3)

    assert (
        intersect.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users INTERSECT SELECT main_users.fullname FROM public.main_users INTERSECT SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )

    expected_result = [{"fullname": "Python"}, {"fullname": "Qaspen"}]
    assert (
        await intersect.transaction_execute(
            transaction=test_db_transaction,
        )
        == expected_result
    )
