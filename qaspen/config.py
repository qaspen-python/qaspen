from __future__ import annotations

from pathlib import Path
from typing import Final, Literal, Optional, TypedDict, cast

from pydantic import BaseModel
from tomlkit import TOMLDocument, parse


class TypedQaspenConfig(TypedDict):
    """TypedDict for typing proposes."""

    engine_path: str | None


class QaspenConfig:
    """Small wrapper for qaspen config."""

    _config: _QaspenConfig | None = None

    @classmethod
    def config(cls: type[QaspenConfig]) -> _QaspenConfig:
        """Return config.

        If config doesn't exist, build new one.
        If config exists, return it.
        """
        if cls._config:
            return cls._config

        cls._config = _QaspenConfig._build_config()
        return cls._config


class _QaspenConfig(BaseModel):
    """Main config for qaspen library.

    It can be filled with parameters from `pyproject.toml`
    or `qaspen_config.toml`.

    For now, main priority has `qaspen_config.toml`.
    """

    config_type: Optional[  # noqa: UP007
        Literal["pyproject", "qaspen_config"]
    ] = None
    engine_path: Optional[str] = None  # noqa: UP007

    @classmethod
    def _build_config(
        cls: type[_QaspenConfig],
    ) -> _QaspenConfig:  # noqa: PYI019
        """Build main Qaspen config.

        It can be built from internal qaspen config
        with name `qaspen_config.toml` or from
        `pyproject.toml` with `[tool.qaspen]` section.

        Config could be not found.
        """
        try:
            return cls._process_config()
        except (KeyError, OSError):
            pass

        try:
            return cls._process_pyproject_config()
        except (KeyError, OSError):
            pass

        return _QaspenConfig()

    @classmethod
    def _process_config(
        cls: type[_QaspenConfig],
    ) -> _QaspenConfig:  # noqa: PYI019
        qaspen_toml_config = cls._open_config(
            config_type="/qaspen_config.toml",
        )
        qaspen_config: Final = cast(
            TypedQaspenConfig,
            qaspen_toml_config["settings"],
        )
        return _QaspenConfig(
            **qaspen_config,
            config_type="qaspen_config",
        )

    @classmethod
    def _process_pyproject_config(
        cls: type[_QaspenConfig],
    ) -> _QaspenConfig:  # noqa: PYI019
        pyproject_toml = cls._open_config(
            config_type="/pyproject.toml",
        )
        qaspen_config: Final = cast(
            TypedQaspenConfig,
            pyproject_toml["tool"]["qaspen"],  # type: ignore[index]
        )

        return _QaspenConfig(
            **qaspen_config,
            config_type="pyproject",
        )

    @classmethod
    def _open_config(
        cls: type[_QaspenConfig],
        config_type: str,
    ) -> TOMLDocument:
        with Path(str(Path.cwd()) + config_type).open() as config_file:
            return parse(string=config_file.read())
