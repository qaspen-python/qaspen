import copy
import dataclasses
from typing import Any, Dict, Final, List, Optional, Type

from typing_extensions import Self

from qaspen.engine.base import BaseEngine
from qaspen.fields.base_field import EmptyFieldValue
from qaspen.fields.fields import Field


@dataclasses.dataclass
class MetaTableData:
    table_name: str = ""
    abstract: bool = False
    table_fields: Dict[str, Field[Any]] = dataclasses.field(
        default_factory=dict,
    )
    database_engine: Optional[BaseEngine[Any, Any]] = None
    alias: Optional[str] = None


class MetaTable:
    _table_meta: MetaTableData = MetaTableData()
    _subclasses: List[Type["MetaTable"]] = []

    def __init_subclass__(
        cls: Type["MetaTable"],
        table_name: Optional[str] = None,
        abstract: bool = False,
        **kwargs: Any,
    ) -> None:
        if not table_name:
            table_name = cls.__name__.lower()

        table_fields: Final[Dict[str, Field[Any]],] = cls._parse_table_fields()

        cls._table_meta = MetaTableData(
            table_name=table_name,
            abstract=abstract,
            table_fields=table_fields,
        )

        cls._subclasses.append(cls)

        super().__init_subclass__(**kwargs)

    def __init__(self: Self, **fields_values: Any) -> None:
        for table_field in self._table_meta.table_fields.values():
            setattr(
                self,
                table_field.original_field_name,
                copy.deepcopy(table_field),
            )
            new_field_value: Any = fields_values.get(
                table_field.original_field_name,
                EmptyFieldValue(),
            )
            setattr(
                self,
                table_field.original_field_name,
                new_field_value,
            )

    def __getattr__(  # TODO: We I implemented it?
        self: Self,
        attribute: str,
    ) -> Any:
        return self.__dict__[attribute]

    def __getattribute__(self, attribute: str) -> Any:
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
            return object.__getattribute__(self, attribute).value
        return super().__getattribute__(attribute)

    @classmethod
    def _parse_table_fields(
        cls: Type["MetaTable"],
    ) -> Dict[str, Field[Any]]:
        table_fields: Final[Dict[str, Field[Any]]] = {
            field_class._field_data.field_name: field_class
            for field_class in cls.__dict__.values()
            if isinstance(field_class, Field)
        }

        return table_fields

    @classmethod
    def _retrieve_not_abstract_subclasses(
        cls: Type["MetaTable"],
    ) -> List[Type["MetaTable"]]:
        return [
            subclass
            for subclass in cls._subclasses
            if not subclass._table_meta.abstract
        ]
