import logging as _logging
from typing import (
    Optional as _Optional,
    Union as _Union,
    overload as _overload,
)

from pyspark.sql import Column as _Column, functions as _fns
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)
from wheely.mammoth.proteins import (
    ProteinDataset as _ProteinDataset,
)

_logger = _logging.getLogger(__name__)


@_overload
def filter_psms(
    dataset: _PsmDataset,
    threshold_col: _Optional[_Union[str, _Column]],
    qval_thresh: _Optional[float],
    include_decoys: _Optional[bool],
) -> _PsmDataset: ...


@_overload
def filter_psms(
    dataset: _ProteinDataset,
    threshold_col: _Optional[_Union[str, _Column]],
    qval_thresh: _Optional[float],
    include_decoys: _Optional[bool],
) -> _ProteinDataset: ...


def filter_psms(
    dataset,
    threshold_col: _Optional[_Union[str, _Column]],
    qval_thresh: _Optional[float],
    include_decoys: _Optional[bool],
):
    """
    Return a dataset containing only filtered PSMs, as follows:

    If the optional `threshold_col` parameter is provided, only rows where this column
    is `True` will be included in the output. If `threshold_col` is not specified but the dataset is
    a `wheely.mammoth.ConfidenceDataset` or a `wheelyy.mammoth.proteins.ProteinConfidenceDataset` the optional
    `qval_thresh` parameter will be used to filter PSMs or proteins. Otherwise all target rows in the dataset
    will be included in the output. To include decoys in the output, pass `include_decoys=True`
    (note: `include_decoys` is ignored if `threshold_col` is specified).

    Parameters
    ----------
    dataset: ConfidenceDataset
        The dataset to write
    threshold_col (str | pyspark.sql.Column; optional): A column (or its name) specifying which
        rows will be included in the resulting library.
    qval_thresh (float): The largest _q_-value accepted into the library. Ignored if
        the dataset is not a `wheely.mammoth.ConfidenceDataset` or `threshold_col` is specified.
    include_decoys (bool; default = False): If true, include decoy PSMs in the library. Ignored
        if `threshold_col` is specified.
    """

    if threshold_col is None:
        if (qval_col := getattr(dataset, "qvalue_column", None)) is not None:
            if qval_thresh is not None:
                _logger.info(
                    "Filtering PSMs with q-value threshold %f and decoys %scluded",
                    qval_thresh,
                    "in" if include_decoys else "ex",
                )

                return dataset.with_data(
                    dataset.data.filter(
                        (_fns.col(qval_col) <= qval_thresh)
                        & (dataset.targets | _fns.lit(include_decoys))
                    )
                )
        elif qval_thresh is not None:
            _logger.warning(
                "Dataset lacks confidence estimates! Ignoring qval_thresh=%f",
                qval_thresh,
            )

        # No filtering possible
        _logger.warning(
            "No threshold for PSM filtering! Output will be unfiltered, with decoys %scluded",
            "in" if include_decoys else "ex",
        )
        return dataset.with_data(
            dataset.data.filter(dataset.targets | _fns.lit(include_decoys))
        )

    _logger.info(
        "Filtering PSMs with threshold: %s",
        threshold_col,
    )

    return dataset.with_data(dataset.data.filter(threshold_col))
