import typing

from qaspen.exceptions import DatabaseUrlError


def parse_database(database_url: str) -> str:
    """Parse database from connection url.

    ### Returns:
    Connection from connection url.
    """
    try:
        return database_url.split("/")[-1]
    except LookupError as exc:
        parsing_error_message: typing.Final = (
            "Database url cannot be parsed, it's empty"
        )
        raise DatabaseUrlError(parsing_error_message) from exc
