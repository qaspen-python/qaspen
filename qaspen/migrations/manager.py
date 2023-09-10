import dataclasses
import typing

from qaspen.engine.base_engine import BaseEngine
from qaspen.migrations.migration import Migration


@dataclasses.dataclass
class MigrationManagerMeta:
    """Meta information for migration manager."""

    db_engine: BaseEngine[typing.Any]


class MigrationManager:
    """Main class of the migration.

    It creates new migrations, compare tables, do all the stuff.
    """

    _migration_manager_meta: MigrationManagerMeta

    def __init__(self: typing.Self) -> None:
        pass

    def init_db(self: typing.Self) -> None:
        """Initialize the database.

        Create new table with name `qaspen`.
        This table will be used as a migration-data table.
        """

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
