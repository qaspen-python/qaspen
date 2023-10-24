import abc

from typing_extensions import Self

from qaspen.querystring.querystring import QueryString


class SQLSelectable(abc.ABC):
    @abc.abstractmethod
    def querystring(self: Self) -> QueryString:
        ...
