"""
scry: Extreme-scale proteomics pipeline

Exports:

- TODO
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
# from .package import Symbol  # import relative to this package to avoid namespace collisions
