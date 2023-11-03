import abc

from typing_extensions import Protocol, Self, runtime_checkable

from qaspen.querystring.querystring import QueryString


@runtime_checkable
class SQLSelectable(Protocol):
    """Protocol for any object that can be used in SQL query."""

    @abc.abstractmethod
    def querystring(self: Self) -> QueryString:
        """Create new QueryString.

        QueryString is the main SQL query building class.
        It can concatenate with different delimiters.

        ### Returns
        `QueryString` or it's subclass.
        """
        ...
