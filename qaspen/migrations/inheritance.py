"""Classes for tables and fields to support migrations."""
import abc

from qaspen.migrations.operations.base_operation import Operation
from qaspen.querystring.querystring import QueryString


class MigrationCreate(abc.ABC):
    """Class for create operation in migrations.

    Each table, field, constraint and index can be created.
    """

    @classmethod
    @abc.abstractmethod
    def _create_operation(cls: type["MigrationCreate"]) -> Operation:
        pass


class MigrationDelete(abc.ABC):
    """Class for delete operation in migrations.

    Each table, field, constraint and index can be deleted.
    """

    @classmethod
    @abc.abstractmethod
    def _delete_entity_statement(cls: type["MigrationDelete"]) -> QueryString:
        pass


class MigrationUpdate(abc.ABC):
    """Class for update operation in migrations.

    Table and field can be changed.
    For example we can change the name.
    """

    @classmethod
    @abc.abstractmethod
    def _update_entity_statement(cls: type["MigrationUpdate"]) -> QueryString:
        pass
