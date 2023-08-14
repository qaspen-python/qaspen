import abc
import typing

from qaspen.querystring.querystring import QueryString


class BaseStatement(abc.ABC):
    """Base statement all statements."""

    @abc.abstractmethod
    def querystring(self: typing.Self) -> QueryString:
        ...
