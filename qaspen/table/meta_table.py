import dataclasses
import typing

from qaspen.fields.fields import Field


@dataclasses.dataclass
class MetaTableData:
    table_name: str = ""
    abstract: bool = False
    table_fields: list[Field[typing.Any]] = dataclasses.field(
        default_factory=list,
    )


class MetaTable:

    _table_meta: MetaTableData = MetaTableData()

    def __init_subclass__(
        cls: type["MetaTable"],
        table_name: str | None = None,
        abstract: bool = False,
        **kwargs: typing.Any,
    ) -> None:
        if abstract:
            super().__init_subclass__(**kwargs)
            return

        if not table_name:
            table_name = cls.__name__.lower()

        cls.abstract = abstract  # type: ignore[attr-defined]
        table_fields: typing.Final[
            list[Field[typing.Any]],
        ] = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            abstract=abstract,
            table_fields=table_fields,
        )

        super().__init_subclass__(**kwargs)

    def __init__(self: typing.Self, **new_fields_values: typing.Any) -> None:
        for table_field in self._table_meta.table_fields:
            new_field_value: typing.Any | None = new_fields_values.get(
                table_field._field_name,
            )
            if new_field_value:
                setattr(self, table_field._field_name, new_field_value)

    @classmethod
    def _parse_table_fields(cls: type["MetaTable"]) -> list[Field[typing.Any]]:
        table_fields: typing.Final[list[Field[typing.Any]]] = [
            field_class
            for field_class
            in cls.__dict__.values()
            if isinstance(field_class, Field)
        ]

        for table_field in table_fields:
            setattr(cls, table_field._field_name, table_field)

        return table_fields
