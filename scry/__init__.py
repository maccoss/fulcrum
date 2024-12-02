"""
scry: Extreme-scale proteomics pipeline

Exports:

- :py:func:`~scry.scry.scry` -- programmatic entry point for Scry workflows
"""

# Initialize the package.
try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("scry-ms")
    except PackageNotFoundError:
        pass

except ImportError:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        __version__ = get_distribution("scry-ms").version
    except DistributionNotFound:
        pass

# Here is where we can export public functions and classes.
from .scry import scry
