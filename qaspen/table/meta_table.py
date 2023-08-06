import typing

from qaspen.field.base_field import BaseField


class MetaTable:
    """"""

    table_name: str
    abstract: bool
    table_fields: list[BaseField]

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
            cls.table_name = cls.__name__.lower()
        else:
            cls.table_name = table_name

        cls.abstract = abstract
        cls._parse_table_fields()

        super().__init_subclass__(**kwargs)

    @classmethod
    def _parse_table_fields(cls: type["MetaTable"]) -> None:
        table_fields: typing.Final[list[BaseField]] = [
            field_class
            for field_class
            in cls.__dict__.values()
            if isinstance(field_class, BaseField)
        ]

        cls.table_fields = table_fields

    class Config:
        extra = "allow"
