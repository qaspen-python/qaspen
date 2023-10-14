import copy
import typing

from qaspen.fields.base_field import BaseField
from qaspen.fields.fields import Field
from qaspen.statements.select_statement import SelectStatement
from qaspen.table.meta_table import MetaTable

T_ = typing.TypeVar(
    "T_",
    bound="type[BaseTable]",
)


class BaseTable(
    MetaTable,
    abstract=True,
):
    @classmethod
    def select(
        cls: T_,
        select_fields: typing.Iterable[BaseField[typing.Any]] | None = None,
    ) -> SelectStatement:
        """Create SelectStatement based on table.

        :param select_fields: fields to select. By default select all possible fields.

        :returns: SelectStatement.
        """
        if not select_fields:
            select_fields = cls.all_fields()
        select_statement: typing.Final[SelectStatement] = SelectStatement(
            select_fields=select_fields,
            from_table=cls,
        )
        return select_statement

    @classmethod
    def all_fields(cls: type["BaseTable"]) -> list[BaseField[typing.Any]]:
        """Return all fields in the table.

        :returns: list of fields.
        """
        return cls._table_meta.table_fields

    @classmethod
    def aliased(cls: T_, alias: str) -> T_:
        """Add alias to the table.

        It'll be used in queries instead of real table name.

        :param alias: string alias to the table.

        :returns: the same table but with the alias.
        """
        copied_table: typing.Final = copy.deepcopy(cls)
        copied_table._table_meta.alias = alias
        return copied_table

    @classmethod
    def is_aliased(cls: type["BaseTable"]) -> bool:
        """Return flag that says does the table have an alias.

        :returns: boolean flag.
        """
        return bool(cls._table_meta.alias)

    @classmethod
    def original_table_name(cls: type["BaseTable"]) -> str:
        return cls._table_meta.table_name

    @classmethod
    def table_name(cls: type["BaseTable"]) -> str:
        """Return the original table name or alias to it.

        :returns: original table name or alias.
        """
        return cls._table_meta.alias or cls.original_table_name()

    @classmethod
    def _retrieve_field(
        cls: type["BaseTable"],
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
