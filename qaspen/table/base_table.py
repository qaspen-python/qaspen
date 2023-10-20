import copy
import inspect
import typing

from qaspen.fields.base_field import BaseField, FieldType
from qaspen.fields.fields import Field
from qaspen.statements.select_statement import SelectStatement
from qaspen.table.meta_table import MetaTable

T_ = typing.TypeVar(
    "T_",
    bound="BaseTable",
)


class BaseTable(
    MetaTable,
    abstract=True,
):
    @classmethod
    def select(
        cls: type[T_],
        *select_fields: BaseField[FieldType],
    ) -> SelectStatement[T_]:
        """Create SelectStatement based on table.

        :param select_fields: fields to select. By default select all possible fields.

        :returns: SelectStatement.
        """
        select_statement: typing.Final[SelectStatement[T_]] = SelectStatement(
            select_fields=select_fields or cls.all_fields(),
            from_table=cls,
        )
        return select_statement

    @classmethod
    def all_fields(cls: type[T_]) -> list[BaseField[FieldType]]:
        """Return all fields in the table.

        :returns: list of fields.
        """
        return [*cls._table_meta.table_fields.values()]

    @classmethod
    def aliased(cls: type[T_], alias: str) -> type[T_]:
        """Create aliased version of the Table.

        ### Parameters
        :param alias: alias to the field.

        ### Returns
        :returns: Same Table, but aliased.

        Example:
        ------
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
    def _aliased(cls: type[T_], alias: str) -> type[T_]:
        """Add alias to the table.

        We must create new `class Table` because in other
        way `alias` change will affect all other queries where
        aliased table is used.

        It'll be used in queries instead of real table name.

        :param alias: string alias to the table.

        :returns: the same table but with the alias.
        """

        class Table(cls):  # type: ignore[valid-type, misc]
            pass

        attributes = inspect.getmembers(
            cls,
            lambda member: not (inspect.isroutine(member)),
        )
        only_field_attributes: dict[str, BaseField[typing.Any]] = {
            attribute[0]: copy.deepcopy(attribute[1])
            for attribute in attributes
            if issubclass(type(attribute[1]), BaseField)
        }

        for table_param_name, table_param in cls.__dict__.items():
            setattr(Table, table_param_name, table_param)

        for field_name, field in only_field_attributes.items():
            field._field_data.from_table = Table
            setattr(Table, field_name, field)

        Table._table_meta = copy.deepcopy(cls._table_meta)
        Table._table_meta.alias = alias

        for field in Table._table_meta.table_fields.values():
            field._field_data.from_table = Table

        return Table

    @classmethod
    def is_aliased(cls: type[T_]) -> bool:
        """Return flag that says does the table have an alias.

        :returns: boolean flag.
        """
        return bool(cls._table_meta.alias)

    @classmethod
    def original_table_name(cls: type[T_]) -> str:
        return cls._table_meta.table_name

    @classmethod
    def table_name(cls: type[T_]) -> str:
        """Return the original table name or alias to it.

        :returns: original table name or alias.
        """
        return cls._table_meta.alias or cls.original_table_name()

    @classmethod
    def _retrieve_field(
        cls: type[T_],
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
