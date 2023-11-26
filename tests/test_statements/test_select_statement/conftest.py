from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.fields.primitive import IntegerField, VarCharField
from qaspen.table.base_table import BaseTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgTransaction


class UserTable(BaseTable, table_name="main_users"):
    """Table for test `SelectStatement`."""

    user_id = IntegerField()
    fullname = VarCharField()


class ProfileTable(BaseTable, table_name="profiles"):
    """Table for test `SelectStatement`."""

    user_id = IntegerField()
    profile_id = IntegerField()
    nickname = VarCharField()


class VideoTable(BaseTable, table_name="videos"):
    """Table for test `SelectStatement`."""

    profile_id = IntegerField()
    video_id = IntegerField()
    count = IntegerField()


@pytest.fixture()
async def _create_test_data(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Create two tables and insert data in it.

    Create the tables shown above:
    - UserTable
    - ProfileTable
    - VideoTable
    """
    await create_user_table(
        test_db_transaction=test_db_transaction,
    )
    await create_profile_table(
        test_db_transaction=test_db_transaction,
    )
    await create_video_table(
        test_db_transaction=test_db_transaction,
    )

    await test_db_transaction.execute(
        fetch_results=False,
        querystring=(
            """
            INSERT INTO main_users VALUES
            (1, 'Qaspen'),
            (2, 'Python');
            """
        ),
    )

    await test_db_transaction.execute(
        fetch_results=False,
        querystring=(
            """
            INSERT INTO profiles VALUES
            (1, 1, 'ORM'),
            (2, 2, 'PL');
            """
        ),
    )

    await test_db_transaction.execute(
        fetch_results=False,
        querystring=(
            """
            INSERT INTO videos VALUES
            (1, 1, 10),
            (2, 2, 100);
            """
        ),
    )


async def create_user_table(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Create `UserTable`."""
    await test_db_transaction.execute(
        fetch_results=False,
        querystring=(
            """
            CREATE TABLE main_users (
                user_id INTEGER,
                fullname VARCHAR
            );
            """
        ),
    )


async def create_profile_table(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Create `ProfileTable`."""
    await test_db_transaction.execute(
        fetch_results=False,
        querystring=(
            """
            CREATE TABLE profiles (
                user_id INTEGER,
                profile_id INTEGER,
                nickname VARCHAR
            )
            """
        ),
    )


async def create_video_table(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Create `ProfileTable`."""
    await test_db_transaction.execute(
        fetch_results=False,
        querystring=(
            """
            CREATE TABLE videos (
                profile_id INTEGER,
                video_id INTEGER,
                count INTEGER
            )
            """
        ),
    )
