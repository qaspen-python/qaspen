from typing import TYPE_CHECKING, Generic, runtime_checkable

from typing_extensions import Protocol, Self

from qaspen.qaspen_types import ComparisonT

if TYPE_CHECKING:
    from qaspen.querystring.querystring import QueryString


@runtime_checkable
class SQLSelectable(Protocol):
    """Protocol for any object that can be used in SQL query."""

    def querystring(self: Self) -> "QueryString":
        """Create new QueryString.

        QueryString is the main SQL query building class.
        It can concatenate with different delimiters.

        ### Returns
        `QueryString` or it's subclass.
        """
        ...  # pragma: no cover


class SQLComparison(
    SQLSelectable,
    Generic[ComparisonT],
):
    """This class is used for inheritance for all comparison classes.

    For example, for the class that makes custom logic for __eq__ class.

    It's necessary to have this class because it will be used as a Type
    in places where can be used only subclass of comparison.

    As an example, `Filter` class in its first argument must accept only
    subclasses of this class.
    """
