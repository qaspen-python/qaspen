import enum
import typing


class OperationType(enum.StrEnum):
    """Statement operations."""

    SELECT: typing.Final[str] = "select"
    UPDATE: typing.Final[str] = "update"
    DELETE: typing.Final[str] = "delete"
    INSERT: typing.Final[str] = "insert"
