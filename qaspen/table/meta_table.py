import dataclasses
import typing
from qaspen.engine.base_engine import BaseEngine

from qaspen.fields.base.base_field import BaseField
from qaspen.fields.fields import Field


@dataclasses.dataclass
class MetaTableData:
    table_name: str = ""
    abstract: bool = False
    table_fields: list[BaseField[typing.Any]] = dataclasses.field(
        default_factory=list,
    )
    database_engine: BaseEngine[typing.Any] | None = None


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
            list[BaseField[typing.Any]],
        ] = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            abstract=abstract,
            table_fields=table_fields,
        )

        super().__init_subclass__(**kwargs)

    def __init__(self: typing.Self, **fields_values: typing.Any) -> None:
        for table_field in self._table_meta.table_fields:
            new_field_value: typing.Any | None = fields_values.get(
                table_field.field_name_clear,
            )
            if new_field_value:
                setattr(
                    self,
                    table_field.field_name_clear,
                    new_field_value,
                )

    def __getattr__(
        self: typing.Self,
        attribute: str,
    ) -> typing.Any:
        return self.__dict__[attribute]

    @classmethod
    def get_field(
        cls: type["MetaTable"],
        field_name: str,
    ) -> Field[typing.Any]:
        try:
            return typing.cast(
                Field[typing.Any],
                cls.__dict__[field_name],
            )
        except LookupError:
            raise AttributeError(
                f"Table `{cls.__name__}` doesn't have `{field_name}` field",
            )

    @classmethod
    def _table_name(cls: type["MetaTable"]) -> str:
        return cls._table_meta.table_name

    @classmethod
    def _parse_table_fields(
        cls: type["MetaTable"],
    ) -> list[BaseField[typing.Any]]:
        table_fields: typing.Final[list[BaseField[typing.Any]]] = [
            field_class
            for field_class
            in cls.__dict__.values()
            if isinstance(field_class, BaseField)
        ]

        for table_field in table_fields:
            setattr(cls, table_field.field_name_clear, table_field)

        return table_fields
