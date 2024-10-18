"""
`scry.output.confidence`: output implementations for confidence results
"""

from typing import (
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import (
    Column as _Column,
)

from wheely.mammoth import PsmDataset as _PsmDataset

from .util import filter_psms


def write_csv(
    data: _PsmDataset,
    location: str,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    **kwargs,
):
    """
    Write the given dataset to CSV. A thin wrapper around `pyspark.sql.DataFrameWrite.csv`.

    Filtering -- If the optional `threshold_col` parameter is provided, only rows where this column
    is `True` will be included in the output. If `threshold_col` is not specified but the dataset is
    a `wheely.mammoth.ConfidenceDataset` the optional `qval_thresh` parameter will be used to filter
    PSMs. Otherwise all target PSMs in the dataset will be included in the output. To include decoys
    in the output, pass `include_decoys=True` (note: `include_decoys` is ignored if `threshold_col`
    is specified).

    Parameters
    ----------
    dataset: ConfidenceDataset
        The dataset to write
    location: A string specifying where the results should be written
    threshold_col (str | pyspark.sql.Column; optional): A column (or its name) specifying which
        rows will be included in the resulting library.
    qval_thresh (float): The largest _q_-value accepted into the library. Ignored if
        the dataset is not a `wheely.mammoth.ConfidenceDataset` or `threshold_col` is specified.
    include_decoys (bool; default = False): If true, include decoy PSMs in the library. Ignored
        if `threshold_col` is specified.
    **kwargs: additional keyword arguments to pass to `pyspark.sql.DataFrameWrite.csv`.
              Defaults: `{"mode": "errorifexists", "header": True}`
    """

    data_filt = filter_psms(
        data,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
    )

    _kwargs = kwargs.copy()

    data_filt.data.write.csv(
        path=location,
        mode=_kwargs.pop("mode", "errorifexists"),
        header=_kwargs.pop("header", True),
        **_kwargs,
    )
