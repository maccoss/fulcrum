"""
`scry.output.library.spectra.utils` -- utility methods for handling spectral library peaklists
"""

from typing import Union as _Union

import pandas as _pd

from pyspark.sql import Column as _Column
from pyspark.sql.functions import (
    PandasUDFType as _PandasUDFType,
    array as _array,
    col as _col,
    explode as _explode,
    pandas_udf as _pandas_udf,
    transform as _transform,
)

from .dataset import PeaklistType as _PeaklistType


def lists_to_peaklist(
    mz_col: _Union[str, _Column], inten_col: _Union[str, _Column]
) -> _Column:
    """
    Convert two array-typed columns of M/Z and intensity values into an appropriately-structured
    single "peaklist" column.
    """
    raise NotImplementedError()


@_pandas_udf(returnType=_PeaklistType, functionType=_PandasUDFType.GROUPED_AGG)
def rows_to_peaklist(mz_col: _pd.Series, inten_col: _pd.Series) -> _pd.Series:
    """
    This Pandas UDF can be applied to a dataframe of individual peaks, grouped into spectra.
    For each group it will return a single row with all peaks combined into a list of pairs.
    """
    raise NotImplementedError()


@_pandas_udf(returnType=_PeaklistType, functionType=_PandasUDFType.GROUPED_AGG)
def pairs_to_peaklist(pair_col: _pd.Series) -> _pd.Series:
    """
    This Pandas UDF can be applied to a dataframe of individual peaks, grouped into spectra.
    For each group it will return a single row with all peaks combined into a list of pairs.
    """
    return _pd.Series(list(pair_col))


def peaklist_to_lists(peaklist_col: _Union[str, _Column]) -> _Column:
    """
    Convert a single "peaklist" column into two array-typed columns of M/Z and intensity values.
    """
    raise NotImplementedError()


def peaklist_to_pairs(peaklist_col: _Union[str, _Column]) -> _Column:
    """
    Convert a single "peaklist" column into a single array-valued column with a row for each peak in
    each of the input spectra. Each row will contain a two-element array giving (m/z, intensity).

    Note that due to restrictions in Spark SQL it's difficult to split these pairs into separate
    columns. It's typically best to use this pair-based format and `pairs_to_peaklist` than to
    rely on `rows_to_peaklist`.
    """
    return _explode(_col(peaklist_col))
