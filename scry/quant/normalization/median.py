"""
Implementations of median normalization.
"""

from pyspark.sql import (
    Column as _Column,
    functions as _fns,
    Window as _Window,
)

from wheely.mammoth import PsmIntensityDataset as _PsmIntensityDataset

from .base import BasicNormalizer
from .util import get_filtered_intensities as _get_filtered_intensities


class MedianNormalizer(BasicNormalizer):
    """
    Computes median normalization
    """

    def get_normalized_column(
        self,
        dataset: _PsmIntensityDataset,
        *_,
        qval_thresh=None,
        include_decoys=False,
    ) -> _Column:
        """
        Return a :py:class:`~pyspark.sql.Column` that computes median normalization.

        Parameters
        ----------
        dataset : PsmIntensityDataset
            The dataset to normalize.
        qval_thresh : float, optional
            If specified, the median will be computed from only PSMs with *q*-values less than or equal to this value.
        include_decoys : bool, optional
            If ``False`` (default), the median will be computed from only target PSMs.
        """
        if _:
            raise TypeError("Unsupported: additional positional arguments!")

        if qval_thresh is not None:
            intensities = _get_filtered_intensities(
                dataset,
                qval_thresh=qval_thresh,
                include_decoys=include_decoys,
            )
        else:
            intensities = dataset.intensities

        return (
            dataset.intensities
            / _fns.median(intensities).over(
                _Window.partitionBy(dataset.samples)
            )
            * _fns.median(intensities)
        )
