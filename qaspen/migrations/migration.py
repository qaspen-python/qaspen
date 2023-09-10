import datetime
import uuid

from qaspen.migrations.operations.base_operation import Operation


class Migration:
    """Migration for the migration files."""

    MIGRATION_ID: uuid.UUID
    CREATED_AT: datetime.datetime
    APPLY_OPERATIONS: list[Operation]
    ROLLBACK_OPERATIONS: list[Operation]
