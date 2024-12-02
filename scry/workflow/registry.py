"""
`scry.workflow.registry`: registry for pluggable workflows
"""

import logging as _logging
from typing import Callable as _Callable

# Once the min supported version reaches 3.10, the standard library should
# be used like so -> from importlib.metadata import entry_points
from importlib_metadata import entry_points

from .v0 import scry_v0
from .v1 import scry_v1

_logger = _logging.getLogger(__name__)

_workflows = {
    "v0": scry_v0,
    "scry_v0": scry_v0,
    "v1": scry_v1,
    "scry_v1": scry_v1,
}
_plugins = None


def register_workflow(name: str, workflow: _Callable, clobber=False):
    """
    Register a callable with the given name

    A workflow can be easily implemented by defining a function with the signature::

        def my_workflow(spark: SparkSession, **kwargs):
           ...

    This workflow can then be registered::

        scry.workflow.register_workflow(my_workflow)

    """

    assert callable(workflow)

    if name in _workflows:
        if not clobber:
            raise RuntimeError(
                f"Workflow {name} is already registered and `clobber` is False"
            )

        _logger.warning(
            f"Replacing already-registered workflow {name} with {workflow}"
        )

    _workflows[name] = workflow


def _get_plugins():
    """Return a dict of all installed Plugins as {name: EntryPoint}."""

    plugins = entry_points(group="scry.workflow.plugins")

    pluginmap = {}
    for plugin in plugins:
        pluginmap[plugin.name] = plugin

    for k, v in pluginmap.items():
        _logger.debug(f"loading {k}")
        pluginmap[k] = v.load()

    return pluginmap


def get_workflow(name):
    """Fetch a workflow with the given name."""
    global _workflows, _plugins
    try:
        return _workflows[name]
    except KeyError as e:
        if _plugins is None:
            _plugins = _get_plugins()

        if _plugins is not None and name in _plugins:
            return _plugins[name]

        all_keys = set(_workflows.keys())
        if _plugins is not None:
            all_keys = all_keys.union(_plugins.keys())

        raise KeyError(
            f"No such workflow {name}. Only {str(all_keys)} are supported"
        ) from e
