"""Classes for tables and fields to support migrations."""
import abc
import inspect
import typing

from qaspen.querystring.querystring import QueryString

if typing.TYPE_CHECKING:
    from qaspen.migrations.operations.base_operation import Operation


class MigrationCreate(abc.ABC):
    """Class for create operation in migrations.

    Each table, field, constraint and index can be created.
    """

    @classmethod
    @abc.abstractmethod
    def _create_operation(cls: type["MigrationCreate"]) -> "Operation":
        pass


class MigrationDelete(abc.ABC):
    """Class for delete operation in migrations.

    Each table, field, constraint and index can be deleted.
    """

    @classmethod
    @abc.abstractmethod
    def _delete_entity_statement(cls: type["MigrationDelete"]) -> QueryString:
        pass


class MigrationUpdate(abc.ABC):
    """Class for update operation in migrations.

    Table and field can be changed.
    For example we can change the name.
    """

    @classmethod
    @abc.abstractmethod
    def _update_entity_statement(cls: type["MigrationUpdate"]) -> QueryString:
        pass


class ClassAsString(abc.ABC):
    """Class that allows class instance to be turned into a string.

    For example:
    ------
    You have a class:
    ```
    class MyTable(ClassAsString):
        def __init__(self, name: str) -> None:
            self.name = name

        def turn_into_string(self) -> str:
            ...


    my_table = MyTable("GoodGood")
    my_table.turn_into_string()
    ```
    `turn_into_string` will create string like `MyTable(name="GoodGood")`
    """

    @property
    def migration_class_name(self: typing.Self) -> str:
        return self.__class__.__name__

    def turn_into_string(self: typing.Self) -> str:
        parameters_name = [
            parameter
            for parameter in inspect.signature(
                self.__class__.__init__,
            ).parameters.keys()
            if parameter not in ("self", "args", "kwargs")
        ]
        args_dict = {}
        for arg_name in parameters_name:
            value = self.__dict__.get(arg_name)
            args_dict[arg_name] = value

        args_str = ", ".join(
            f"{key}={value.__repr__()}" for key, value in args_dict.items()
        )

        return f"{self.migration_class_name}({args_str})"
