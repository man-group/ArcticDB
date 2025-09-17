from types import ModuleType
from typing import Any, Tuple
from importlib import import_module


class MissingModule(ModuleType):
    """
    A dummy module used to represent a missing optional dependency.
    Raises a meaningful error message when trying to get any attribute.
    """

    def __init__(
        self,
        module_name: str,
    ) -> None:
        self._module_name = module_name
        super().__init__(module_name)

    def __getattr__(self, name: str) -> Any:
        msg = f"ArcticDB's {self._module_name!r} optional dependency is missing but should be installed to use this feature."
        raise ModuleNotFoundError(msg)


def _import_optional_dependency(module_name: str) -> Tuple[ModuleType, bool]:
    try:
        module = import_module(module_name)
        return module, True
    except ImportError:
        module = MissingModule(module_name)
        return module, False


pyarrow, _PYARROW_AVAILABLE = _import_optional_dependency("pyarrow")


__all__ = [
    "pyarrow",
    "_PYARROW_AVAILABLE",
]
