from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.test_statements.conftest import UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_exists_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` `ExistsStatement` method."""
    stmt = (
        UserTable.select(
            UserTable.fullname,
        )
        .where(UserTable.fullname == "Qaspen")
        .exists()
    )

    with pytest.raises(expected_exception=AttributeError):
        await stmt

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    assert (
        stmt.querystring_for_select().build()
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = 'Qaspen')"  # noqa: E501
    )

    assert await stmt


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_exists_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` `ExistsStatement` method."""
    test_engine.running_transaction.set(test_db_transaction)

    stmt = (
        UserTable.select(
            UserTable.fullname,
        )
        .where(UserTable.fullname == "Qaspen")
        .exists()
    )

    assert (
        stmt.querystring_for_select().build()
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = 'Qaspen')"  # noqa: E501
    )

    assert await stmt.execute(
        engine=test_engine,
    )


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_exists_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `transaction_execute` `ExistsStatement` method."""
    stmt = (
        UserTable.select(
            UserTable.fullname,
        )
        .where(UserTable.fullname == "Qaspen")
        .exists()
    )

    assert (
        stmt.querystring_for_select().build()
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = 'Qaspen')"  # noqa: E501
    )

    assert await stmt.transaction_execute(
        transaction=test_db_transaction,
    )


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_exists_as_subquery_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `ExistsStatement` as a subquery."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).where(
        UserTable.select()
        .where(
            UserTable.fullname == "Qaspen",
        )
        .exists(),
    )

    assert (
        stmt.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users WHERE EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = 'Qaspen')"  # noqa: E501
    )

    assert await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
