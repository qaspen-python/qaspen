from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.test_statements.conftest import UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


@pytest.mark.anyio()
@pytest.mark.usefixtures(
    "_create_test_data",
    "_mock_find_engine",
)
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

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    querystring, qs_params = stmt.querystring_for_select().build()
    assert (
        querystring
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = %s)"  # noqa: E501
    )
    assert qs_params == ["Qaspen"]

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

    querystring, qs_params = stmt.querystring_for_select().build()
    assert (
        querystring
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = %s)"  # noqa: E501
    )
    assert qs_params == ["Qaspen"]

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

    querystring, qs_params = stmt.querystring_for_select().build()
    assert (
        querystring
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = %s)"  # noqa: E501
    )
    assert qs_params == ["Qaspen"]

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

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users WHERE EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = %s)"  # noqa: E501
    )
    assert qs_params == ["Qaspen"]

    assert await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
