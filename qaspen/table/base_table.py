import typing

from qaspen.field.base_field import BaseField
from qaspen.field.string_fields.varchar import VarCharField
from qaspen.statement.statement import Statement
from qaspen.table.meta_table import MetaTable


class BaseTable(MetaTable):

    @classmethod
    def select(cls: type["BaseTable"]) -> Statement:
        new_statement: Statement = Statement._select(
            select_fields=list(cls.table_fields),  # type: ignore[attr-defined]
            from_table=cls,
        )
        return new_statement

    @classmethod
    def update(
        cls: type["BaseTable"],
        to_update_fields: dict[BaseField, typing.Any],
    ) -> Statement:
        new_statement: Statement = Statement._update(
            to_update_fields=to_update_fields,
            from_table=cls,
        )
        return new_statement


class Test2(BaseTable):
    aba: VarCharField = VarCharField()
    not_aba: VarCharField = VarCharField()


print(Test2.select().build_query())
print(Test2.update({Test2.aba: "123", Test2.not_aba: "555"}).build_query())
