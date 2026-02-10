"""
registry for pluggable output backends
"""

import logging as _logging

# Once the min supported version reaches 3.10, the standard library should
# be used like so -> from importlib.metadata import entry_points
from importlib_metadata import entry_points

from .basic import write_csv, write_parquet
from .combined import write_combined
from .library import write_library

_logger = _logging.getLogger(__name__)

_backends = {
    "write_csv": write_csv,
    "write_parquet": write_parquet,
    "combined": write_combined,
    "write_library": write_library,
}
_plugins = None


def register_backend(name, backend, clobber=False):
    assert callable(backend)

    if name in _backends:
        if not clobber:
            raise RuntimeError(
                f"Backend {name} is already registered and `clobber` is False"
            )

        _logger.warning(
            f"Replacing already-registered backend {name} with {backend}"
        )

    _backends[name] = backend


def _get_plugins():
    """Return a dict of all installed Plugins as {name: EntryPoint}."""

    plugins = entry_points(group="fulcrum.output.plugins")

    pluginmap = {}
    for plugin in plugins:
        pluginmap[plugin.name] = plugin

    for k, v in pluginmap.items():
        _logger.debug(f"loading {k}")
        pluginmap[k] = v.load()

    return pluginmap


def get_backend(name):
    """Fetch a backend with the given name."""
    global _plugins
    try:
        return _backends[name]
    except KeyError as e:
        if _plugins is None:
            _plugins = _get_plugins()

        if _plugins is not None and name in _plugins:
            return _plugins[name]

        all_keys = set(_backends.keys())
        if _plugins is not None:
            all_keys = all_keys.union(_plugins.keys())

        raise KeyError(
            f"No such backend {name}. Only {str(all_keys)} are supported"
        ) from e
