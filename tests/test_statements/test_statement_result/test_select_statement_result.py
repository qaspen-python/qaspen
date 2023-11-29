from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from msgspec import Struct
from pydantic import BaseModel

from tests.test_statements.conftest import UserTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgTransaction


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_result_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `result` method in `SelectStatementResult`."""
    stmt = UserTable.select()

    result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    assert result.result() == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_to_pydantic_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `to_pydantic` method in `SelectStatementResult`."""

    class UserPydantic(BaseModel):
        """PydanticModel for statement result."""

        user_id: int
        fullname: str

    stmt = UserTable.select()

    result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        UserPydantic(user_id=1, fullname="Qaspen"),
        UserPydantic(user_id=2, fullname="Python"),
    ]

    assert (
        result.to_pydantic(
            pydantic_model=UserPydantic,
        )
        == expected_result
    )


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_to_msgspec_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `to_msgspec` method in `SelectStatementResult`."""

    class UserMsgSpec(Struct):
        """MsgspecStruct for statement result."""

        user_id: int
        fullname: str

    stmt = UserTable.select()

    result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        UserMsgSpec(user_id=1, fullname="Qaspen"),
        UserMsgSpec(user_id=2, fullname="Python"),
    ]

    assert (
        result.to_msgspec(
            msgspec_struct=UserMsgSpec,
        )
        == expected_result
    )
