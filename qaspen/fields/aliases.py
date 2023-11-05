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
            self.alias_field_map: dict[str, FieldAlias] = {}
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
            self.last_alias_number += 1

            alias: Final = f"A{self.last_alias_number}"
            new_aliased_field: Final = field._with_alias(alias=alias)

            self.alias_field_map[f"A{self.last_alias_number}"] = FieldAlias(
                aliased_field=new_aliased_field,
            )
            return new_aliased_field

        def __str__(self: Self) -> str:
            return str(self.alias_field_map)

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
            self.last_alias_number += 1

            alias: Final = f"A{self.last_alias_number}"
            new_aliased_field: Final = field._with_alias(alias=alias)

            self.data[f"A{self.last_alias_number}"] = FieldAlias(
                aliased_field=new_aliased_field,
            )
            return new_aliased_field

        def __str__(self: Self) -> str:
            return str(self.data)
