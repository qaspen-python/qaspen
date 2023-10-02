"""Manager and entrypoint for work with migrations."""
import dataclasses
import os
import typing

import jinja2
import psycopg

from qaspen.engine.base import BaseEngine
from qaspen.migrations.migration import Migration
from qaspen.migrations.settings import Settings
from qaspen.querystring.querystring import QueryString
from qaspen.table.base_table import BaseTable
from qaspen.table.meta_table import MetaTable

TEMPLATE_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "migration_template",
)

JINJA_ENV = jinja2.Environment(
    loader=jinja2.FileSystemLoader(searchpath=TEMPLATE_DIRECTORY),
)


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
        template = JINJA_ENV.get_template("template.py.jinja")
        exist_tables: list[BaseTable] = typing.cast(
            list[BaseTable],
            MetaTable._retrieve_not_abstract_subclasses(),
        )
        content = template.render(
            apply_operations=[
                f"{exist_table._create_operation().string_representation},"
                for exist_table in exist_tables
            ],
        )
        print(content)

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
