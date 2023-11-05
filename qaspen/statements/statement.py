import abc

from typing_extensions import Self

from qaspen.querystring.querystring import QueryString


class BaseStatement(abc.ABC):
    """Base statement all statements."""

    @abc.abstractmethod
    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        ...
