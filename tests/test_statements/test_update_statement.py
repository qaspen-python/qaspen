from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.exceptions import FieldDeclarationError
from qaspen.statements.update_statement import UpdateStatement
from tests.test_statements.conftest import ForTestTable, UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


def test_update_stmt_init_method() -> None:
    """Test `init` method.

    Checks that all instance attributes have expected
    default values.
    """
    update_stmt = UpdateStatement(
        from_table=ForTestTable,
        for_update_map={
            ForTestTable.name: "qaspen",
        },
    )

    assert update_stmt._from_table == ForTestTable
    assert update_stmt._for_update_map == {
        ForTestTable.name: "qaspen",
    }
    assert not update_stmt._is_where_used
    assert not update_stmt._force
    assert not update_stmt._returning


def test_update_stmt_querystring_method() -> None:
    """Test `querystring` method."""
    update_stmt = (
        UpdateStatement(
            from_table=ForTestTable,
            for_update_map={
                ForTestTable.name: "qaspen",
            },
        )
        .where(
            ForTestTable.count == "1",
        )
        .returning(
            ForTestTable.name,
        )
    )

    querystring, qs_params = update_stmt.querystring().build()
    assert (
        querystring
        == "UPDATE fortesttable SET name = %s WHERE fortesttable.count = %s RETURNING fortesttable.name"  # noqa: E501
    )
    assert qs_params == ["qaspen", "1"]


def test_update_stmt_force_method() -> None:
    """Test `force` method."""
    update_stmt = UpdateStatement(
        from_table=ForTestTable,
        for_update_map={
            ForTestTable.name: "qaspen",
        },
    )

    with pytest.raises(expected_exception=FieldDeclarationError):
        update_stmt.querystring()

    update_stmt = update_stmt.force()
    update_stmt.querystring()


def test_update_stmt_deforce_method() -> None:
    """Test `deforce` method."""
    update_stmt = UpdateStatement(
        from_table=ForTestTable,
        for_update_map={
            ForTestTable.name: "qaspen",
        },
    ).force()

    update_stmt = update_stmt.deforce()

    with pytest.raises(expected_exception=FieldDeclarationError):
        update_stmt.querystring()


@pytest.mark.usefixtures(
    "_create_test_data",
)
@pytest.mark.anyio()
async def test_update_stmt_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` method."""
    update_stmt = (
        UpdateStatement(
            from_table=UserTable,
            for_update_map={
                UserTable.fullname: "Alex",
            },
        )
        .where(
            UserTable.user_id == 1,
        )
        .returning(
            UserTable.fullname,
        )
    )

    UserTable._table_meta.database_engine = None

    with pytest.raises(expected_exception=AttributeError):
        await update_stmt

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    res = await update_stmt

    assert res[0]["fullname"] == "Alex"  # type: ignore[index]


@pytest.mark.usefixtures(
    "_create_test_data",
)
@pytest.mark.anyio()
async def test_update_stmt_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` method."""
    update_stmt = (
        UpdateStatement(
            from_table=UserTable,
            for_update_map={
                UserTable.fullname: "Alex",
            },
        )
        .where(
            UserTable.user_id == 1,
        )
        .returning(
            UserTable.fullname,
        )
    )

    test_engine.running_transaction.set(test_db_transaction)

    res = await update_stmt.execute(engine=test_engine)

    assert res[0]["fullname"] == "Alex"  # type: ignore[index]


@pytest.mark.usefixtures(
    "_create_test_data",
)
@pytest.mark.anyio()
async def test_update_stmt_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `transaction_execute` method."""
    update_stmt = (
        UpdateStatement(
            from_table=UserTable,
            for_update_map={
                UserTable.fullname: "Alex",
            },
        )
        .where(
            UserTable.user_id == 1,
        )
        .returning(
            UserTable.fullname,
        )
    )

    res = await update_stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    assert res[0]["fullname"] == "Alex"  # type: ignore[index]
