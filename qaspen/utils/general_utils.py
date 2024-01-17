from __future__ import annotations

import contextlib
import sys
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any, Final, Generator


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
