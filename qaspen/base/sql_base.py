import abc
import typing


class SQLSelectable(abc.ABC):
    @abc.abstractmethod
    def make_sql_string(self: typing.Self) -> str:
        ...
