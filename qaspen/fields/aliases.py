import sys
from collections import UserDict
from typing import Any, Dict, Final

from typing_extensions import Self

from qaspen.fields.fields import Field


class FieldAlias:
    def __init__(
        self: Self,
        aliased_field: Field[Any],
    ) -> None:
        self.aliased_field: Final = aliased_field


if sys.version_info >= (3, 9):

    class FieldAliases(UserDict[str, FieldAlias]):
        def __init__(self: Self):
            self.data: Dict[str, FieldAlias] = {}
            self.last_alias_number: int = 0

        def add_alias(
            self: Self,
            field: Field[Any],
        ) -> Field[Any]:
            self.last_alias_number += 1

            alias: Final = f"A{self.last_alias_number}"
            new_aliased_field: Final = field._with_alias(alias=alias)

            self.data[f"A{self.last_alias_number}"] = FieldAlias(
                aliased_field=new_aliased_field,
            )
            return new_aliased_field

        def __str__(self: Self) -> str:
            return str(self.data)

else:

    class FieldAliases(UserDict):  # type: ignore[type-arg]
        def __init__(self: Self):
            self.data: Dict[str, FieldAlias] = {}
            self.last_alias_number: int = 0

        def add_alias(
            self: Self,
            field: Field[Any],
        ) -> Field[Any]:
            self.last_alias_number += 1

            alias: Final = f"A{self.last_alias_number}"
            new_aliased_field: Final = field._with_alias(alias=alias)

            self.data[f"A{self.last_alias_number}"] = FieldAlias(
                aliased_field=new_aliased_field,
            )
            return new_aliased_field

        def __str__(self: Self) -> str:
            return str(self.data)
