from __future__ import annotations

import contextlib
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Final, Generator, Literal, Optional, TypedDict, cast

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


@contextmanager
def add_cwd_in_path() -> Generator[None, None, None]:
    """Add current directory in python path.

    This context manager adds current directory in sys.path,
    so all python files are discoverable now, without installing
    current project.

    :yield: none
    """
    current_wd: Final = Path.cwd()
    if str(current_wd) in sys.path:
        yield
    else:
        sys.path.insert(0, str(current_wd))
        try:
            yield
        finally:
            with contextlib.suppress(ValueError):
                sys.path.remove(str(current_wd))


def import_object(object_spec: str) -> Any:
    """Parse python object spec and imports it.

    :param object_spec: string in format like `package.module:variable`
    :raises ValueError: if spec has unknown format.
    :returns: imported broker.
    """
    import_spec = object_spec.split(":")
    expected_split_number: Final = 2
    if len(import_spec) != expected_split_number:
        import_msg_err = (
            "You should provide object path in `module:variable` format."
        )
        raise ValueError(import_msg_err)
    with add_cwd_in_path():
        module = import_module(import_spec[0])

    return getattr(module, import_spec[1])
