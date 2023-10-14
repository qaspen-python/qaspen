import copy
import dataclasses
import typing

from qaspen.engine.base import BaseEngine
from qaspen.fields.base_field import BaseField


@dataclasses.dataclass
class MetaTableData:
    table_name: str = ""
    abstract: bool = False
    table_fields: list[BaseField[typing.Any]] = dataclasses.field(
        default_factory=list,
    )
    database_engine: BaseEngine[typing.Any, typing.Any] | None = None
    alias: str | None = None


class MetaTable:
    _table_meta: MetaTableData = MetaTableData()
    _subclasses: list[type["MetaTable"]] = []

    def __init_subclass__(
        cls: type["MetaTable"],
        table_name: str | None = None,
        abstract: bool = False,
        **kwargs: typing.Any,
    ) -> None:
        if not table_name:
            table_name = cls.__name__.lower()

        table_fields: typing.Final[
            list[BaseField[typing.Any]],
        ] = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            abstract=abstract,
            table_fields=table_fields,
        )

        cls._subclasses.append(cls)

        super().__init_subclass__(**kwargs)

    def __init__(self: typing.Self, **fields_values: typing.Any) -> None:
        for table_field in self._table_meta.table_fields:
            setattr(
                self,
                table_field.original_field_name,
                copy.deepcopy(table_field),
            )
            new_field_value: typing.Any | None = fields_values.get(
                table_field.original_field_name,
            )
            if new_field_value:
                setattr(
                    self,
                    table_field.original_field_name,
                    new_field_value,
                )

    def __getattr__(
        self: typing.Self,
        attribute: str,
    ) -> typing.Any:
        return self.__dict__[attribute]

    @classmethod
    def _parse_table_fields(
        cls: type["MetaTable"],
    ) -> list[BaseField[typing.Any]]:
        table_fields: typing.Final[list[BaseField[typing.Any]]] = [
            field_class
            for field_class in cls.__dict__.values()
            if isinstance(field_class, BaseField)
        ]

        return table_fields

    @classmethod
    def _retrieve_not_abstract_subclasses(
        cls: type["MetaTable"],
    ) -> list[type["MetaTable"]]:
        return [
            subclass
            for subclass in cls._subclasses
            if not subclass._table_meta.abstract
        ]
