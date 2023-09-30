import dataclasses
import typing

import psycopg

from qaspen.engine.base_engine import BaseEngine
from qaspen.migrations.migration import Migration
from qaspen.querystring.querystring import QueryString


@dataclasses.dataclass
class MigrationManagerMeta:
    """Meta information for migration manager."""

    db_engine: BaseEngine[typing.Any]


class MigrationManager:
    """Main class of the migration.

    It creates new migrations, compare tables, do all the stuff.
    """

    def __init__(self: typing.Self, db_engine: BaseEngine[typing.Any]) -> None:
        self._migration_manager_meta = MigrationManagerMeta(
            db_engine=db_engine,
        )

    @property
    def _db_engine(self) -> BaseEngine[typing.Any]:
        return self._migration_manager_meta.db_engine

    async def init_db(self: typing.Self) -> None:
        """Initialize the database.

        Create new table with name `qaspen`.
        This table will be used as a migration-data table.
        """
        table_name: typing.Final = "qaspen_metadata"
        is_table_exist = True

        try:
            await self._db_engine.run_query(
                querystring=QueryString(
                    table_name,
                    sql_template="SELECT 1 FROM {}",
                ),
            )
        except psycopg.errors.UndefinedTable:
            is_table_exist = False

        if is_table_exist:
            print(f"Qaspen table {table_name} is already exist in database!")
            return

        create_table_qs: typing.Final = QueryString(
            table_name,
            sql_template="""
            CREATE TABLE {} (
                id SERIAL,
                migration_name VARCHAR(255) NOT NULL,
                is_applied BOOLEAN NOT NULL,
                version VARCHAR(100),
                tables_state JSONB NOT NULL
            )
            """,
        )

        try:
            await self._db_engine.run_query_without_result(
                querystring=create_table_qs,
            )
        except Exception as create_table_exc:
            print(
                f"Impossible to create table due to "
                f"exception - {create_table_exc}",
            )
            return

        print("Database was successfully initialized!")

    def migrate(self: typing.Self) -> None:
        """Create new migrations."""

    def apply(
        self: typing.Self,
        migration: Migration,
    ) -> None:
        """Apply the migration.

        Run all operations in the `apply_operations`
        array.
        Do it in the transaction by default, but it is
        possible to set flag `in_transaction` to `False` in the
        migration to turn off this mechanism.

        :param migration: migration to apply.
        """

    def rollback(
        self: typing.Self,
        migration: Migration,
    ) -> None:
        """Rollback the migration.

        Run all operations in the `rollback_operations`
        array.
        Do it in the transaction by default, but it is
        possible to set flag `in_transaction` to `False` in the
        migration to turn off this mechanism.

        :param migration: migration to rollback.
        """

    def retrieve_migrations(
        self: typing.Self,
        path_to_migrations: str,
    ) -> list[Migration]:
        return []
