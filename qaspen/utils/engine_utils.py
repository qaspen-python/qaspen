import typing
from urllib.parse import urlparse

from qaspen.exceptions import DatabaseUrlParseError


def parse_database(database_url: str) -> str:
    """Parse database from connection url.

    ### Returns:
    Database name from connection url.
    """
    database_name: typing.Final = urlparse(database_url).path[1:]
    if not database_name:
        parsing_error_message: typing.Final = (
            "Database name cannot be parsed, it's empty"
        )
        raise DatabaseUrlParseError(parsing_error_message)

    return database_name
