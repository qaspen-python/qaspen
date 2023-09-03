import typing
from collections import UserDict

from qaspen.fields.base_field import BaseField


class FieldAlias:
    def __init__(
        self: typing.Self,
        aliased_field: BaseField[typing.Any],
    ) -> None:
        self.aliased_field: typing.Final = aliased_field


class FieldAliases(UserDict[str, FieldAlias]):
    def __init__(self: typing.Self):
        self.data: dict[str, FieldAlias] = {}
        self.last_alias_number: int = 0

    def add_alias(
        self: typing.Self,
        field: BaseField[typing.Any],
    ) -> BaseField[typing.Any]:
        self.last_alias_number += 1

        alias: typing.Final = f"A{self.last_alias_number}"
        new_aliased_field: typing.Final = field._with_alias(alias=alias)

        self.data[f"A{self.last_alias_number}"] = FieldAlias(
            aliased_field=new_aliased_field,
        )
        return new_aliased_field

    def __str__(self: typing.Self) -> str:
        return str(self.data)
