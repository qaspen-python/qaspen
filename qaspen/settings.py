import tomllib
import typing


class Settings:
    """Settings for qaspen.

    Parse `pyproject.toml` file and retrieve
    necessary parameters.
    """

    def __init__(self: typing.Self) -> None:
        self.path_to_pyproject: typing.Final = "./pyproject.toml"
        self.pyproject_data: typing.Final = self._retrieve_pyproject_data()

        self.models_file: str | None = self._retrieve_option("models_file")
        self.models_folder: str | None = self._retrieve_option("models_folder")
        self.migrations_folder: str | None = self._retrieve_option(
            "migrations_folder",
        )

    def _retrieve_pyproject_data(self: typing.Self) -> dict[str, typing.Any]:
        with open(self.path_to_pyproject, "rb") as pyproject_file:
            return tomllib.load(pyproject_file)

    def _retrieve_option(
        self: typing.Self,
        option_name: str,
    ) -> typing.Any | None:
        try:
            return self.pyproject_data["tool"]["qaspen"][option_name]
        except IndexError:
            return None
