"""
`scry.output.library.spectra.utils` -- utility methods for handling spectral library peaklists
"""

import pandas as _pd

from pyspark.sql import Column as _Column
from pyspark.sql.functions import (
    PandasUDFType as _PandasUDFType,
    pandas_udf as _pandas_udf,
)

from .dataset import PeaklistType as _PeaklistType


def lists_to_peaklist(mz_col: _Column, inten_col: _Column) -> _Column:
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


def peaklist_to_lists(peaklist_col: _Column) -> _Column:
    """
    Convert a single "peaklist" column into two array-typed columns of M/Z and intensity values.
    """
    raise NotImplementedError()


def peaklist_to_rows(peaklist_col: _Column) -> _pd.Series:
    """
    Convert a single "peaklist" column into multiple rows, each of a single peak, with separate m/z
    and intensity columns.
    TODO: define exact signature
    """
    raise NotImplementedError()
