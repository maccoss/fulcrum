"""
`scry.search.existing`: tools for reading already-existing search results
"""

from typing import (
    Callable as _Callable,
    Iterable as _Iterable,
    Union as _Union,
)

from pyspark.sql import SparkSession as _SparkSession

from wheely.mammoth import PsmDataset as _PsmDataset
from wheely.mammoth.utils import listify as _listify

from wheely.mammoth.parsers import read_encyclopedia_features


_engines = {
    "encyclopedia": read_encyclopedia_features,
}


def read_existing_results(
    engine: _Union[str, _Callable[..., _PsmDataset]],
    file_locations: _Union[str, _Iterable[str]],
    spark: _SparkSession = None,
) -> _PsmDataset:
    """
    `read_existing_results`: read search results with `wheely-mammoth`

    Parameters
    ----------
    engine: str
        The search engine format whose result format will be read.
    file_locations: str, [str]
        One or more locations from which results should be read
    spark: SparkSession, optional
        The SparkSession with which to read results / create DataFrames.
        If not specified a session will be created with default settings!
    """
    if not isinstance(engine, _Callable):
        engine = _engines[engine]

    return engine(_listify(file_locations), spark=spark)
