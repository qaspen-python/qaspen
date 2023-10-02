"""Module with all possible operations in the migrations."""
from qaspen.migrations.operations.create_field import CreateFieldOperation
from qaspen.migrations.operations.create_table import CreateTableOperation

__all__ = [
    "CreateFieldOperation",
    "CreateTableOperation",
]
