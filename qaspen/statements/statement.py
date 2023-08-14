import abc
import typing


class BaseStatement(abc.ABC):
    """Base statement all statements."""

    @abc.abstractmethod
    def querystring(self: typing.Self) -> str:
        ...
