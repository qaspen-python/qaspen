from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.exceptions import ColumnDeclarationError
from qaspen.statements.delete_statement import DeleteStatement
from tests.test_statements.conftest import ForTestTable, UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


def test_delete_stmt_init_method() -> None:
    """Test `init` method.

    Checks that all instance attributes have expected
    default values.
    """
    update_stmt = DeleteStatement(
        from_table=ForTestTable,
    )

    assert update_stmt._from_table == ForTestTable
    assert not update_stmt._is_where_used
    assert not update_stmt._force
    assert not update_stmt._returning


def test_delete_stmt_querystring_method() -> None:
    """Test `querystring` method."""
    delete_stmt = (
        DeleteStatement(
            from_table=ForTestTable,
        )
        .where(
            ForTestTable.count == "1",
        )
        .returning(
            ForTestTable.name,
        )
    )

    querystring, qs_params = delete_stmt.querystring().build()
    assert (
        querystring
        == "DELETE FROM fortesttable WHERE fortesttable.count = %s RETURNING fortesttable.name"  # noqa: E501
    )
    assert qs_params == ["1"]


def test_delete_stmt_force_method() -> None:
    """Test `force` method."""
    delete_stmt = DeleteStatement(
        from_table=ForTestTable,
    )

    with pytest.raises(expected_exception=ColumnDeclarationError):
        delete_stmt.querystring()

    delete_stmt = delete_stmt.force()
    delete_stmt.querystring()


def test_delete_stmt_deforce_method() -> None:
    """Test `deforce` method."""
    delete_stmt = DeleteStatement(
        from_table=ForTestTable,
    ).force()

    delete_stmt = delete_stmt.deforce()

    with pytest.raises(expected_exception=ColumnDeclarationError):
        delete_stmt.querystring()


@pytest.mark.usefixtures(
    "_create_test_data",
    "_mock_find_engine",
)
@pytest.mark.anyio()
async def test_delete_stmt_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` method."""
    delete_stmt = (
        DeleteStatement(
            from_table=UserTable,
        )
        .where(
            UserTable.user_id == 1,
        )
        .returning(
            UserTable.fullname,
        )
    )

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    res = await delete_stmt

    assert res[0]["fullname"] == "Qaspen"  # type: ignore[index]

    assert not (
        (
            await UserTable.select()
            .where(
                UserTable.user_id == 1,
            )
            .execute(engine=test_engine)
        ).result()
    )


@pytest.mark.usefixtures(
    "_create_test_data",
)
@pytest.mark.anyio()
async def test_delete_stmt_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` method."""
    delete_stmt = (
        DeleteStatement(
            from_table=UserTable,
        )
        .where(
            UserTable.user_id == 1,
        )
        .returning(
            UserTable.fullname,
        )
    )

    test_engine.running_transaction.set(test_db_transaction)

    res = await delete_stmt.execute(engine=test_engine)

    assert res[0]["fullname"] == "Qaspen"  # type: ignore[index]

    assert not (
        (
            await UserTable.select()
            .where(
                UserTable.user_id == 1,
            )
            .execute(engine=test_engine)
        ).result()
    )


@pytest.mark.usefixtures(
    "_create_test_data",
)
@pytest.mark.anyio()
async def test_delete_stmt_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `transaction_execute` method."""
    delete_stmt = (
        DeleteStatement(
            from_table=UserTable,
        )
        .where(
            UserTable.user_id == 1,
        )
        .returning(
            UserTable.fullname,
        )
    )

    res = await delete_stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    assert res[0]["fullname"] == "Qaspen"  # type: ignore[index]

    assert not (
        (
            await UserTable.select()
            .where(
                UserTable.user_id == 1,
            )
            .transaction_execute(transaction=test_db_transaction)
        ).result()
    )
