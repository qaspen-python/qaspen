import dataclasses
import typing

from qaspen.fields.base_field import Field

from qaspen.statement.enums import OperationType
from qaspen.table.meta_table import MetaTable


@dataclasses.dataclass
class Statement:
    """"""
    operation: OperationType
    from_table: type[MetaTable]
    select_fields: list[str] = dataclasses.field(default_factory=list)
    update_fields: dict[Field[type], typing.Any] = dataclasses.field(
        default_factory=dict,
    )
    insert_records: tuple[MetaTable, ...] | None = None
    exist_prefixs: list[str] = dataclasses.field(default_factory=list)

    def build_query(self) -> str:
        build_query_operation_map: typing.Final[
            dict[OperationType, typing.Callable[[], str]]
        ] = {
            OperationType.SELECT: self._build_select_query,
            OperationType.UPDATE: self._build_update_query,
        }
        build_query_method: typing.Final[
            typing.Callable[[], str],
        ] = build_query_operation_map[self.operation]
        return build_query_method()

    def _build_select_query(self: typing.Self) -> str:
        to_select_fields: str = ", ".join(self.select_fields)
        string_statement: str = (
            f"SELECT {to_select_fields} "
            f"FROM {self.from_table._table_meta.table_name}"
        )
        return string_statement

    def _build_update_query(self: typing.Self) -> str:
        to_update_fields: typing.Final[str] = ", ".join(
            [
                f"{field._field_name} = {new_value}"
                for field, new_value
                in self.update_fields.items()
            ]
        )
        return (
            f"UPDATE "
            f"{self.from_table._table_meta.table_name} "
            f"SET {to_update_fields}"
        )

    @classmethod
    def _select(
        cls: type["Statement"],
        select_fields: typing.Iterable[Field[typing.Any]],
        from_table: type[MetaTable],
    ) -> "Statement":
        select_fields_with_prefix: typing.Final[list[str]] = [
            f"{from_table._table_meta.table_name}."
            f"{field._field_name}"
            for field in select_fields
        ]
        return Statement(
            operation=OperationType.SELECT,
            select_fields=select_fields_with_prefix,
            from_table=from_table,
        )

    @classmethod
    def _update(
        cls: type["Statement"],
        to_update_fields: dict[Field[typing.Any], typing.Any],
        from_table: type[MetaTable],
    ) -> "Statement":
        return Statement(
            operation=OperationType.UPDATE,
            update_fields=to_update_fields,
            from_table=from_table,
        )

    @classmethod
    def _delete(
        cls: type["Statement"],
        from_table: type[MetaTable],
    ) -> "Statement":
        return Statement(
            operation=OperationType.DELETE,
            from_table=from_table,
        )

    @classmethod
    def _insert(
        cls: type["Statement"],
        insert_records: tuple[MetaTable, ...],
        from_table: type[MetaTable],
    ) -> "Statement":
        return Statement(
            operation=OperationType.INSERT,
            from_table=from_table,
            insert_records=insert_records,
        )
