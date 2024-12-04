"""
`scry.search.existing`: tools for reading already-existing search results
"""

import logging as _logging
from typing import (
    Callable as _Callable,
    Iterable as _Iterable,
    Union as _Union,
)

from wheely.mammoth.parsers.registry import get_reader

# Once the min supported version reaches 3.10, the standard library should
# be used like so -> from importlib.metadata import entry_points
from importlib_metadata import entry_points

from pyspark.sql import SparkSession as _SparkSession

from wheely.mammoth import PsmDataset as _PsmDataset
from wheely.mammoth.utils import listify as _listify

_logger = _logging.getLogger(__name__)


def read_existing_results(
    engine: _Union[str, _Callable[..., _PsmDataset]],
    location: _Union[str, _Iterable[str]],
    spark: _SparkSession = None,
    **kwargs,
) -> _PsmDataset:
    """
    read search results with ``wheely-mammoth``

    The ``engine`` parameter specifies the parser or plugin that will be used
    to read from ``location`` (a string or list of strings).

    To use, run ``scry`` using the following TOML::

        [search]
        backend = "read_existing"
        engine = "…"
        location = ["…"]

    or invoke :py:func:`scry.scry` with equivalent parameters.

    Parameters
    ----------
    engine : str
        The search engine format whose result format will be read. The name
        of a plugin in the ``wheely-mamoth`` parser registry.
    location : str, [str]
        One or more locations from which results should be read. These should
        be local file paths, or URIs understood by Spark.
    spark : SparkSession, optional
        The SparkSession with which to read results / create DataFrames.
        If not specified a session will be created with default settings!
    """
    _engine: _Callable[..., _PsmDataset]

    if not callable(engine):
        _engine = get_reader(engine)
    else:
        _engine = engine

    return _engine(_listify(location), spark=spark, **kwargs)
