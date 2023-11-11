from __future__ import annotations

import sys
from collections import UserDict
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field


class FieldAlias:
    """Class for field alias.

    It represents single alias.
    """

    def __init__(
        self: Self,
        aliased_field: Field[Any],
    ) -> None:
        """Initialize field alias.

        ### Parameters:
        - `aliased_field`: any Field.
        """
        self.aliased_field: Final = aliased_field


if sys.version_info >= (3, 9):

    class FieldAliases(UserDict[str, FieldAlias]):
        """Class for all aliases."""

        def __init__(self: Self) -> None:
            self.data: dict[str, FieldAlias] = {}

        def add_alias(
            self: Self,
            field: Field[Any],
        ) -> Field[Any]:
            """Add new alias for the field.

            ### Parameters:
            - `field`: any Field.

            ### Returns:
            Field with an alias.
            """
            field_alias: str
            aliased_field: Field
            if exists_alias := field.alias:
                field_alias = exists_alias
                aliased_field = field
            else:
                aliased_field = field.with_alias(
                    alias_name=field._original_field_name,
                )
                field_alias = aliased_field.alias

            self.data[field_alias] = FieldAlias(
                aliased_field=field,
            )
            return field

        def __str__(self: Self) -> str:
            return str(self.data)

else:

    class FieldAliases(UserDict):  # type: ignore[type-arg]
        """Class for all aliases."""

        def __init__(self: Self) -> None:
            self.data: dict[str, FieldAlias] = {}
            self.last_alias_number: int = 0

        def add_alias(
            self: Self,
            field: Field[Any],
        ) -> Field[Any]:
            """Add new alias for the field.

            ### Parameters:
            - `field`: any Field.

            ### Returns:
            Field with an alias.
            """
            field_alias: str
            aliased_field: Field[Any]
            if exists_alias := field.alias:
                field_alias = exists_alias
                aliased_field = field
            else:
                aliased_field = field.with_alias(
                    alias_name=field._original_field_name,
                )
                field_alias = aliased_field.alias  # type: ignore[assignment]

            self.data[field_alias] = FieldAlias(
                aliased_field=field,
            )
            return field

        def __str__(self: Self) -> str:
            return str(self.data)
