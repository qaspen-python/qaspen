import dataclasses
import typing
from qaspen.fields.base_field import Field
from qaspen.table.meta_table import MetaTable


class UpdateStatement:
    update_fields: dict[Field[typing.Any], typing.Any]

    def __init__(
        self: typing.Self,
        update_fields: dict[Field[typing.Any], typing.Any],
        from_table: type[MetaTable],
    ) -> None:
        """"""
        self.to_update_fields: typing.Final[
            dict[Field[typing.Any], typing.Any]
        ] = update_fields
        self.from_table: typing.Final[type[MetaTable]] = from_table

    def build_query(self) -> str:
        to_update_fields: typing.Final[str] = ", ".join(
            [
                f"{field._field_name} = {new_value}"
                for field, new_value
                in self.to_update_fields.items()
            ]
        )
        return (
            f"UPDATE "
            f"{self.from_table._table_meta.table_name} "
            f"SET {to_update_fields}"
        )
