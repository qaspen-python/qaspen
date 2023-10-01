import dataclasses
import typing

import psycopg

from qaspen.engine.base import BaseEngine
from qaspen.migrations.migration import Migration
from qaspen.migrations.settings import Settings
from qaspen.querystring.querystring import QueryString


@dataclasses.dataclass
class MigrationManagerMeta:
    """Meta information for migration manager."""

    db_engine: BaseEngine[typing.Any, typing.Any]


class MigrationManager:
    """Main class of the migration.

    It creates new migrations, compare tables, do all the stuff.
    """

    def __init__(
        self: typing.Self,
        db_engine: BaseEngine[typing.Any, typing.Any],
    ) -> None:
        self.metadata_table: typing.Final = "qaspen_metadata"
        self._migration_manager_meta = MigrationManagerMeta(
            db_engine=db_engine,
        )
        self.settings: typing.Final = Settings()

    @property
    def _db_engine(self) -> BaseEngine[typing.Any, typing.Any]:
        return self._migration_manager_meta.db_engine

    async def init_db(self: typing.Self) -> None:
        """Initialize the database.

        Create new table with name `qaspen_metadata`.
        This table will be used as a migration-data table.
        """
        is_table_exist = True

        try:
            await self._db_engine.run_query(
                querystring=QueryString(
                    self.metadata_table,
                    sql_template="SELECT 1 FROM {}",
                ),
            )
        except psycopg.errors.UndefinedTable:
            is_table_exist = False

        if is_table_exist:
            print(
                f"Qaspen table {self.metadata_table} "
                f"is already exist in database!",
            )
            return

        create_table_qs: typing.Final = QueryString(
            self.metadata_table,
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

    async def migrate(self: typing.Self) -> None:
        """Create new migrations."""
        retrieve_last_migration_info_qs: typing.Final = QueryString(
            sql_template="""
                SELECT
                    migration_name,
                    is_applied,
                    version,
                    tables_state
                FROM
                    {}
                ORDER BY id DESC
            """,
        )

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
