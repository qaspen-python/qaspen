import dataclasses
import typing

from qaspen.engine.base_engine import BaseEngine


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
        pass

    def migrate(self: typing.Self) -> None:
        pass

    def apply(self: typing.Self) -> None:
        pass

    def rollback(self: typing.Self) -> None:
        pass
