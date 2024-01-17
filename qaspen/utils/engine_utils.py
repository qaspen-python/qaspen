from __future__ import annotations

from inspect import isfunction
from typing import TYPE_CHECKING, Any, Callable, Final
from urllib.parse import urlparse

from qaspen.config import QaspenConfig
from qaspen.exceptions import DatabaseUrlParseError
from qaspen.utils.general_utils import import_object

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


class EngineFinder:
    """Class for performing engine search.

    Searching performs only once per application starts.
    """

    found_engine: BaseEngine[Any, Any, Any] | None = None
    is_searched: bool = False

    @classmethod
    def engine(cls: type[EngineFinder]) -> BaseEngine[Any, Any, Any] | None:
        """Try to find an engine.

        If there was a search before return the result, otherwise
        try to find an engine.

        ### Returns:
        Engine for executing queries or None.
        """
        if cls.is_searched:
            return cls.found_engine

        cls.found_engine = cls._find_engine()
        cls.is_searched = True
        return cls.found_engine

    @classmethod
    def _find_engine(
        cls: type[EngineFinder],
    ) -> BaseEngine[Any, Any, Any] | None:
        """Try to find engine based on user config.

        ### Returns:
        Engine for executing queries or None.
        """
        qaspen_config = QaspenConfig.config()

        if not qaspen_config.engine_path:
            return None

        engine_object: (
            BaseEngine[Any, Any, Any] | Callable[[], BaseEngine[Any, Any, Any]]
        ) = import_object(
            qaspen_config.engine_path,
        )

        if isfunction(engine_object):
            return engine_object()  # type: ignore[no-any-return]

        return engine_object  # type: ignore[return-value]
