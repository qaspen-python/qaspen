from __future__ import annotations

from pathlib import Path

from tomlkit import TOMLDocument, parse


class QaspenConfig:
    """Main config for qaspen library.

    It can be filled with parameters from `pyproject.toml`
    or `qaspen_config.toml`.

    For now, main priority has `qaspen_config.toml`.
    """

    engine_path: str | None = None

    master_engine_path: str | None = None
    slave_engine_path: str | None = None

    _pyproject_path: str = "/pyproject.toml"
    _qaspen_config_path: str = "/qaspen_config.toml"
    _is_config_processed: bool = False

    @classmethod
    def build_config(cls: type[QaspenConfig]) -> QaspenConfig:
        """Build main Qaspen config.

        It can happen only once per running application due to caching.
        """
        return QaspenConfig()

    @classmethod
    def _process_config(cls: type[QaspenConfig]) -> QaspenConfig:
        return QaspenConfig()

    @classmethod
    def _process_pyproject_config(cls: type[QaspenConfig]) -> QaspenConfig:
        return QaspenConfig()

    @classmethod
    def _open_config(
        cls: type[QaspenConfig],
        config_type: str,
    ) -> TOMLDocument:
        with Path(str(Path.cwd()) + config_type).open() as config_file:
            return parse(string=config_file.read())
