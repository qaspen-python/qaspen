from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlparse

from qaspen.exceptions import DatabaseUrlParseError

if TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine


def parse_database(database_url: str) -> str:
    """Parse database from connection url.

    ### Returns:
    Database name from connection url.
    """
    database_name: Final = urlparse(database_url).path[1:]
    if not database_name:
        parsing_error_message: Final = (
            "Database name cannot be parsed, it's empty"
        )
        raise DatabaseUrlParseError(parsing_error_message)

    return database_name


def find_engine() -> BaseEngine[Any, Any, Any] | None:
    """Try to find engine based on user config."""
    return None
