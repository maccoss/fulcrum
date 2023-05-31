"""
`scry.output.library` -- workflow module that supports transforming sets of PSMs into a format
suitable for use with library search tools.
"""

from typing import Optional as _Optional

from pyspark.sql import Column as _Column

from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)


def write_library(
    data: _PsmDataset,
    location: str,
    threshold_col: _Optional[str | _Column] = None,
    qval_thresh: float = 0.01,
):
    """
    Write the given dataset to the given location, formatted for use as a spectral library.

    Filtering -- If the optional `threshold_col` parameter is provided, only rows where this column
    is `True` will be included in the output. If `theshold_col` is not specified but the datasest is
    a `wheely.mammoth.ConfidenceDataset` the optional `qval_thresh` parameter will be used to filter
    PSMs. Otherwise all PSMs in the dataset will be included in the output.

    This implementation is still in progress:
    * TODO: define the format in which the library will be written
    * TODO: define the source of spectral / RT information

    Future directions:
    * TODO: add support for other dimensions: e.g. IM

    Parameters
    ----------
    data: The dataset
    location: The output location (path or URI)
    threshold_col (str | pyspark.sql.Column; optional): A column (or its name) specifying which
        rows will be included in the resulting library.
    qval_thresh (float; optional): The largest _q_-value accepted into the library. Ignored if the
        dataset is not a `wheely.mammoth.ConfidenceDataset` or `threshold_col` is specified.

    Returns
    -------
    `None` on success
    """

    # 1. Filter
    raise NotImplementedError("TODO")  # TODO

    # 2. Join spectral info
    # TODO

    # 3. Write output
    # TODO

    # 4. Return
    return None
