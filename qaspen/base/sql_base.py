import abc
import typing

from qaspen.querystring.querystring import QueryString


class SQLSelectable(abc.ABC):
    @abc.abstractmethod
    def querystring(self: typing.Self) -> QueryString:
        ...
