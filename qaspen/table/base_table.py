import typing

from qaspen.fields.base_field import Field
from qaspen.statement.statement import Statement
from qaspen.table.meta_table import MetaTable


class BaseTable(MetaTable):
    @classmethod
    def select(
        cls: type["BaseTable"],
        select_fields: list[Field[typing.Any]],
    ) -> Statement:
        return Statement._select(
            select_fields=select_fields,
            from_table=cls,
        )

    @classmethod
    def update(
        cls: type["BaseTable"],
        to_update_fields: dict[Field[typing.Any], typing.Any],
    ) -> Statement:
        return Statement._update(
            to_update_fields=to_update_fields,
            from_table=cls,
        )

    @classmethod
    def delete(cls: type["BaseTable"]) -> Statement:
        return Statement._delete(
            from_table=cls,
        )

    @classmethod
    def insert(
        cls: type["BaseTable"],
        *insert_records: "BaseTable",
    ) -> Statement:
        return Statement._insert(
            from_table=cls,
            insert_records=insert_records,
        )

    @classmethod
    def all_fields(cls: type["BaseTable"]) -> list[Field[typing.Any]]:
        return cls._table_meta.table_fields
