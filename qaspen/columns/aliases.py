from __future__ import annotations

from collections import UserDict
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.columns.base import Column


class ColumnAlias:
    """Class for column alias.

    It represents single alias.
    """

    def __init__(
        self: Self,
        aliased_column: Column[Any],
    ) -> None:
        """Initialize column alias.

        ### Parameters:
        - `aliased_column`: any Column.
        """
        self.aliased_column: Final = aliased_column


class ColumnAliases(UserDict):  # type: ignore[type-arg]
    """Class for all aliases."""

    def __init__(self: Self) -> None:
        self.data: dict[str, ColumnAlias] = {}

    def add_alias(
        self: Self,
        column: Column[Any],
    ) -> Column[Any]:
        """Add new alias for the column.

        ### Parameters:
        - `column`: any Column.

        ### Returns:
        Column with an alias.
        """
        column_alias: str
        aliased_column: Column[Any]
        if exists_alias := column.alias:
            column_alias = exists_alias
            aliased_column = column
        else:
            aliased_column = column.with_alias(
                alias_name=column._original_column_name,
            )
            column_alias = aliased_column.alias  # type: ignore[assignment]

        self.data[column_alias] = ColumnAlias(
            aliased_column=column,
        )
        return column

    def __str__(self: Self) -> str:
        return str(self.data)  # pragma: no cover
