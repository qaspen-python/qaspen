"""Module with all possible operations in the migrations."""
from qaspen.migrations.operations.field_operations import CreateFieldOperation
from qaspen.migrations.operations.table_operations import CreateTableOperation

__all__ = [
    "CreateFieldOperation",
    "CreateTableOperation",
]
