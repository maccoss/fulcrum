"""
Utility functions for normalization.
"""

from pyspark.sql import (
    Column as _Column,
    functions as _fns,
)

from wheely.mammoth import PsmIntensityDataset as _PsmIntensityDataset


def get_filtered_intensities(
    dataset: _PsmIntensityDataset,
    qval_thresh: float = None,
    include_decoys: bool = False,
) -> _Column:
    """
    Compute a column that returns filtered intensities -- the original value for rows passing the threshold,
    and ``null`` for all others.

    Parameters
    ----------
    dataset : PsmIntensityDataset
        The dataset. Must be a :py:class:`~wheely.mammoth.PsmIntensityConfidenceDataset`
        when ``qval_thresh`` is not ``None``.
    qval_thresh : float, optional
        The threshold at which intensities will be filtered in the returned column. When specified,
        ``dataset`` must be a :py:class:`~wheely.mammoth.PsmIntensityConfidenceDataset`.
    include_decoys : bool, optional
        If ``False`` (default) only target PSMs will be included in the returned column. If ``True``,
        all intensities, including those of decoys will be returned.
    """

    if qval_thresh is None or qval_thresh >= 1.0:
        # No q-value filtering, so qvalue column is not required
        qvalues = _fns.lit(1.0)
        qval_thresh = 1.0
    else:
        qvalues = _fns.col(getattr(dataset, "qvalue_column"))

    return _fns.when(
        (qvalues <= qval_thresh)
        & (dataset.targets | _fns.lit(include_decoys)),
        dataset.intensites,
    ).otherwise(_fns.lit(None))
