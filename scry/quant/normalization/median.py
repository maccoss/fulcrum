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

    def __init__(self):
        self.__name__ = "median"

    def get_normalized_column(
        self,
        dataset: _PsmIntensityDataset,
        *_,
        qval_thresh=None,
        include_decoys=False,
        global_scaling_relative_error: float = 0.001,
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
        global_scaling_relative_error : float, optional
            The relative error allowed when estimating the global scaling factor applied to normalized intensities.
            Passed to :py:func:`pyspark.sql.DataFrame.approxQuantile`.
            Default: 0.001
        """
        if _:
            raise TypeError("Unsupported: additional positional arguments!")

        intensities = _get_filtered_intensities(
            dataset,
            qval_thresh=qval_thresh if qval_thresh is not None else 1.0,
            include_decoys=include_decoys,
        )

        # After scaling intensities to the median, we will scale them globally to maintain the same
        # overall magnitude of the intensities. The exact value used for global scaling is unimportant,
        # so we use an efficient estimate of the overall median, with accuracy controlled by the
        # `global_scaling_relative_error` parameter.
        global_median = dataset.data.select(
            intensities.alias("intensity")
        ).approxQuantile(
            "intensity", [0.5], relativeError=global_scaling_relative_error
        )[
            0
        ]

        return (
            dataset.intensities
            / _fns.median(intensities).over(
                _Window.partitionBy(dataset.samples)
            )
            * _fns.lit(global_median)
        )


class MedianDenseNormalizer(BasicNormalizer):
    """
    Computes median normalization
    """

    def __init__(self):
        super().__init__()
        self.__name__ = "mediandense"

    def get_normalized_column(
        self,
        dataset: _PsmIntensityDataset,
        *_,
        qval_thresh=None,
        include_decoys=False,
        density_thresh: float = 0.8,
        global_scaling_relative_error: float = 0.001,
    ) -> _Column:
        """
        Return a :py:class:`~pyspark.sql.Column` that computes median normalization using only precursors with a density
        above a threshold, as computed by the number of samples with detections.

        Parameters
        ----------
        dataset : PsmIntensityDataset
            The dataset to normalize.
        qval_thresh : float, optional
            If specified, the median will be computed from only PSMs with *q*-values less than or equal to this value.
        include_decoys : bool, optional
            If ``False`` (default), the median will be computed from only target PSMs.
        density_thresh : float, optional
            The density level required for precursors to be used for normalization. Default: 0.8
        global_scaling_relative_error : float, optional
            The relative error allowed when estimating the global scaling factor applied to normalized intensities.
            Passed to :py:func:`pyspark.sql.DataFrame.approxQuantile`.
            Default: 0.001
        """
        if _:
            raise TypeError("Unsupported: additional positional arguments!")

        if getattr(dataset, "charge_column", None) is None:
            raise TypeError("MedianDenseNormalizer requires a charge_column!")

        intensities = _get_filtered_intensities(
            dataset,
            qval_thresh=qval_thresh if qval_thresh is not None else 1.0,
            include_decoys=include_decoys,
        )

        n_samples = (
            dataset.data.select(_fns.countDistinct(dataset.samples))
            .toPandas()
            .iloc[0, 0]
        )

        intensities = _fns.when(
            _fns.count(dataset.samples).over(
                _Window.partitionBy(dataset.peptides, dataset.charges)
            )
            >= density_thresh * n_samples,
            intensities,
        ).otherwise(_fns.lit(None))

        # After scaling intensities to the median, we will scale them globally to maintain the same
        # overall magnitude of the intensities. The exact value used for global scaling is unimportant,
        # so we use an efficient estimate of the overall median, with accuracy controlled by the
        # `global_scaling_relative_error` parameter.
        global_median = dataset.data.select(
            intensities.alias("intensity")
        ).approxQuantile(
            "intensity", [0.5], relativeError=global_scaling_relative_error
        )[
            0
        ]

        return (
            dataset.intensities
            / _fns.median(intensities).over(
                _Window.partitionBy(dataset.samples)
            )
            * _fns.lit(global_median)
        )
