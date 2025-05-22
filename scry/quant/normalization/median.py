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


class MedianNormalizer(BasicNormalizer):
    """
    Computes median normalization
    """

    def get_normalized_column(
        self, dataset: _PsmIntensityDataset, *_
    ) -> _Column:
        """
        Return a :py:class:`~pyspark.sql.Column` that computes median normalization.

        IMPORTANT: currently NO FILTERING is applied to the dataset when computing medians!!
        """
        if _:
            raise TypeError("Unsupported: additional positional arguments!")

        return (
            dataset.intensities
            / _fns.median(dataset.intensity_column).over(
                _Window.partitionBy(dataset.samples)
            )
            * _fns.median(dataset.intensity_column)
        )
