import typing

from qaspen.fields.fields import Field
from qaspen.statements.select_statement import SelectStatement
from qaspen.statements.update_statement import UpdateStatement
from qaspen.table.meta_table import MetaTable


class BaseTable(MetaTable):
    @classmethod
    def select(
        cls: type["BaseTable"],
        select_fields: typing.Iterable[Field[typing.Any]],
    ) -> SelectStatement:
        select_statement: typing.Final[SelectStatement] = SelectStatement(
            select_fields=select_fields,
            from_table=cls,
        )
        return select_statement

    @classmethod
    def update(
        cls: type["BaseTable"],
        update_fields: dict[Field[typing.Any], typing.Any],
    ) -> UpdateStatement:
        return UpdateStatement(
            update_fields=update_fields,
            from_table=cls,
        )

    # @classmethod
    # def delete(cls: type["BaseTable"]) -> Statement:
    #     return Statement._delete(
    #         from_table=cls,
    #     )

    # @classmethod
    # def insert(
    #     cls: type["BaseTable"],
    #     *insert_records: "BaseTable",
    # ) -> Statement:
    #     return Statement._insert(
    #         from_table=cls,
    #         insert_records=insert_records,
    #     )

    @classmethod
    def all_fields(cls: type["BaseTable"]) -> list[Field[typing.Any]]:
        return cls._table_meta.table_fields
