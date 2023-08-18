import abc
import typing


class SQLable(abc.ABC):
    """Unable functionality to turn something into the SQL string."""
    @abc.abstractmethod
    def make_sql_string(self: typing.Self) -> str:
        ...
