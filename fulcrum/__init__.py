"""
Fulcrum Pipeline: Framework for extreme-scale proteomics data processing

Exports:

- :py:func:`.fulcrum` -- programmatic entry point for workflows
"""

# Initialize the package.
try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("fulcrum-ms")
    except PackageNotFoundError:
        pass

except ImportError:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        __version__ = get_distribution("fulcrum-ms").version
    except DistributionNotFound:
        pass

# Here is where we can export public functions and classes.
from .fulcrum import fulcrum
