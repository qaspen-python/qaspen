import copy
import dataclasses
import typing

from qaspen.engine.base import BaseEngine
from qaspen.fields.base_field import BaseField, EmptyFieldValue


@dataclasses.dataclass
class MetaTableData:
    table_name: str = ""
    abstract: bool = False
    table_fields: dict[str, BaseField[typing.Any]] = dataclasses.field(
        default_factory=dict,
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
            dict[str, BaseField[typing.Any]],
        ] = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            abstract=abstract,
            table_fields=table_fields,
        )

        cls._subclasses.append(cls)

        super().__init_subclass__(**kwargs)

    def __init__(self: typing.Self, **fields_values: typing.Any) -> None:
        for table_field in self._table_meta.table_fields.values():
            setattr(
                self,
                table_field.original_field_name,
                copy.deepcopy(table_field),
            )
            new_field_value: typing.Any | None = fields_values.get(
                table_field.original_field_name,
                EmptyFieldValue(),
            )
            setattr(
                self,
                table_field.original_field_name,
                new_field_value,
            )

    def __getattr__(  # TODO: We I implemented it?
        self: typing.Self,
        attribute: str,
    ) -> typing.Any:
        return self.__dict__[attribute]

    def __getattribute__(self, attribute: str) -> typing.Any:
        """Return value of the value instead of the instance of the field.

        Value of the field will be returned only
        if we have initialized `instance` of the field,
        not the class.

        ### Params
        :param attribute: the attribute we are accessing.

        ### Returns
        :returns: any attribute of the table.
        """
        table_meta = object.__getattribute__(self, "_table_meta")
        table_fields = object.__getattribute__(table_meta, "table_fields")
        if attribute in table_fields:
            return table_fields[attribute].value
        return super().__getattribute__(attribute)

    @classmethod
    def _parse_table_fields(
        cls: type["MetaTable"],
    ) -> dict[str, BaseField[typing.Any]]:
        table_fields: typing.Final[dict[str, BaseField[typing.Any]]] = {
            field_class._field_data.field_name: field_class
            for field_class in cls.__dict__.values()
            if isinstance(field_class, BaseField)
        }

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
