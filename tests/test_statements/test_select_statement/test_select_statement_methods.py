from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.clauses.order_by import OrderBy
from tests.test_statements.test_select_statement.conftest import UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


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


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_limit_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `limit` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).limit(limit=1)

    assert (
        stmt.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users LIMIT 1"
    )

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        {"fullname": "Qaspen"},
    ]
    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_offset_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `offset` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).offset(offset=1)

    assert (
        stmt.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users OFFSET 1"
    )

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_limit_offset_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `limit_offset` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).limit_offset(
        limit=1,
        offset=1,
    )

    assert (
        stmt.querystring().build()
        == "SELECT main_users.fullname FROM public.main_users LIMIT 1 OFFSET 1"
    )

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
@pytest.mark.parametrize(
    ("ascending", "nulls_first", "expected_query", "expected_result"),
    [
        (
            True,
            True,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS FIRST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            True,
            False,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS LAST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            False,
            True,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS FIRST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
        (
            False,
            False,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS LAST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
    ],
)
async def test_select_order_by_method_without_order_bys(
    test_db_transaction: PsycopgTransaction,
    ascending: bool,
    nulls_first: bool,
    expected_query: str,
    expected_result: list[dict[str, str]],
) -> None:
    """Test `order_by` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).order_by(
        field=UserTable.fullname,
        ascending=ascending,
        nulls_first=nulls_first,
    )

    assert stmt.querystring().build() == expected_query

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    raw_result = stmt_result.result()
    expected_number_of_results = 2
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
@pytest.mark.parametrize(
    ("order_bys", "expected_query", "expected_result"),
    [
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=True,
                    nulls_first=True,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS FIRST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=True,
                    nulls_first=False,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS LAST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=False,
                    nulls_first=True,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS FIRST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=False,
                    nulls_first=False,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS LAST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
    ],
)
async def test_select_order_by_method_with_order_bys(
    test_db_transaction: PsycopgTransaction,
    order_bys: list[OrderBy],
    expected_query: str,
    expected_result: list[dict[str, str]],
) -> None:
    """Test `order_by` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).order_by(order_bys=order_bys)

    assert stmt.querystring().build() == expected_query

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    raw_result = stmt_result.result()
    expected_number_of_results = 2
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result
