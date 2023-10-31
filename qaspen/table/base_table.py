import copy
import inspect
from typing import Any, Dict, Final, List, Type, TypeVar, cast

from qaspen.fields.base import Field
from qaspen.qaspen_types import FieldType
from qaspen.statements.select_statement import SelectStatement
from qaspen.table.meta_table import MetaTable

T_ = TypeVar(
    "T_",
    bound="BaseTable",
)


class BaseTable(
    MetaTable,
    abstract=True,
):
    @classmethod
    def select(
        cls: Type[T_],
        *select_fields: Field[FieldType],
    ) -> SelectStatement[T_]:
        """Create SelectStatement based on table.

        :param select_fields: fields to select. By default select all possible fields.

        :returns: SelectStatement.
        """
        select_statement: Final[SelectStatement[T_]] = SelectStatement(
            select_fields=select_fields or cls.all_fields(),
            from_table=cls,
        )
        return select_statement

    @classmethod
    def all_fields(cls: Type[T_]) -> List[Field[FieldType]]:
        """Return all fields in the table.

        :returns: list of fields.
        """
        return [*cls._table_meta.table_fields.values()]

    @classmethod
    def aliased(cls: Type[T_], alias: str) -> Type[T_]:
        """Create aliased version of the Table.

        ### Parameters
        :param alias: alias to the field.

        ### Returns
        :returns: Same Table, but aliased.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()
            description: VarCharField = VarCharField()


        # It has the same type with all autosuggestions.
        AliasedBuns = Buns.aliased(alias="PrimaryBuns")
        ```
        """
        return cls._aliased(alias=alias)

    @classmethod
    def _aliased(cls: Type[T_], alias: str) -> Type[T_]:
        """Add alias to the table.

        We must create new `aliased_table` because in other
        way `alias` change will affect all other queries where
        aliased table is used.

        It'll be used in queries instead of real table name.

        :param alias: string alias to the table.

        :returns: the same table but with the alias.
        """
        aliased_table = type(
            cls.__name__,
            (cls,),
            {},
        )

        attributes = inspect.getmembers(
            cls,
            lambda member: not (inspect.isroutine(member)),
        )
        only_field_attributes: Dict[str, Field[Any]] = {
            attribute[0]: copy.deepcopy(attribute[1])
            for attribute in attributes
            if issubclass(type(attribute[1]), Field)
        }

        for table_param_name, table_param in cls.__dict__.items():
            setattr(aliased_table, table_param_name, table_param)

        for field_name, field in only_field_attributes.items():
            field._field_data.from_table = aliased_table
            setattr(aliased_table, field_name, field)

        aliased_table._table_meta = (  # type: ignore[attr-defined]
            copy.deepcopy(
                cls._table_meta,
            )
        )
        aliased_table._table_meta.alias = alias  # type: ignore[attr-defined]

        table_meta_fields = (
            aliased_table._table_meta.table_fields.values()  # type: ignore[attr-defined]
        )
        for field in table_meta_fields:
            field._field_data.from_table = aliased_table

        return aliased_table

    @classmethod
    def is_aliased(cls: Type[T_]) -> bool:
        """Return flag that says does the table have an alias.

        :returns: boolean flag.
        """
        return bool(cls._table_meta.alias)

    @classmethod
    def original_table_name(cls: Type[T_]) -> str:
        return cls._table_meta.table_name

    @classmethod
    def table_name(cls: Type[T_]) -> str:
        """Return the original table name or alias to it.

        :returns: original table name or alias.
        """
        return cls._table_meta.alias or cls.original_table_name()

    @classmethod
    def _retrieve_field(
        cls: Type[T_],
        field_name: str,
    ) -> Field[Any]:
        try:
            return cast(
                Field[Any],
                cls.__dict__[field_name],
            )
        except LookupError:
            raise AttributeError(
                f"Table `{cls.__name__}` doesn't have `{field_name}` field",
            )
