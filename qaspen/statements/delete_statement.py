from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Generic

from qaspen.qaspen_types import FromTable
from qaspen.statements.base import Executable
from qaspen.statements.combinable_statements.filter_statement import (
    FilterStatement,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field
    from qaspen.statements.combinable_statements.combinations import (
        CombinableExpression,
    )


class DeleteStatement(
    BaseStatement,
    Executable[None],
    Generic[FromTable],
):
    """Statement for DELETE queries."""

    def __init__(self: Self, from_table: type[FromTable]) -> None:
        self._from_table: Final = from_table

        self._filter_statement = FilterStatement(
            filter_operator="WHERE",
        )
        self._is_where_used: bool = False
        self._force: bool = False
        self._returning: tuple[Field[Any], ...] | None = None

    def where(
        self: Self,
        *where_arguments: CombinableExpression,
    ) -> Self:
        """Add where clause to the statement.

        It's possible to use this method as many times as you want.
        If you use `where` more than one time, clauses will be connected
        with `AND` operator.

        Fields have different methods for the comparison.
        Also, you can pass the combination of the `where` clauses.

        Below you will see easy and advanced examples.

        One Where Example:
        ------
        ```
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()

        statement = Buns.delete().where(
            Buns.name == "Tasty",
        )
        ```
        """
        self._is_where_used = True
        self._filter_statement.add_filter(*where_arguments)
        return self

    def returning(
        self: Self,
        *returning_field: Field[Any],
    ) -> Self:
        """Add `RETURNING` to the query.

        ### Parameters:
        - `returning_field`: field to return

        ### Returns:
        `self`.
        """
        self._returning = returning_field
        return self

    def force(self: Self) -> Self:
        """Set force flag to True.

        It allows make requests to the database
        without WHERE clause.
        """
        self._force = True
        return self

    def deforce(self: Self) -> Self:
        """Set force flag to False.

        It disallows make requests to the database
        without WHERE clause.
        """
        self._force = False
        return self
