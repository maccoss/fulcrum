"""
Base classes for normalization backends.

Note that any ``callable`` can be used as a backend!
These classes only simplify certain common use cases.
"""

from typing import (
    Callable as _Callable,
)

from pyspark.sql import Column as _Column

from wheely.mammoth import PsmIntensityDataset as _PsmIntensityDataset


class Normalizer:
    def __call__(self):
        raise NotImplementedError("Normalizer can not be used directly! Create an instance of a subclass!")


class BasicNormalizer(Normalizer):
    """
    Base class for normalization implementations that compute a single :py:class:``pyspark.sql.Column``
    from a :py:class:`~wheely.mammoth.PsmIntensityDataset`. Subclasses should implement ``get_normalized_column``;
    this will be included in the returned dataset appropriately.
    """

    def __call__(
        self, dataset: _PsmIntensityDataset, *args, **kwargs
    ) -> _PsmIntensityDataset:
        """
        Call this normalizer on the given dataset.
        """
        if not hasattr(self, "__name__"):
            raise TypeError(
                "BasicNormalizer implementation must have a __name__ attribute!"
            )

        # TODO: allow overriding the column name
        norm_col_name = (
            dataset.intensity_column + "_normalized_" + self.__name__
        )

        return dataset.with_data(
            dataset.data.withColumn(
                norm_col_name,
                self.get_normalized_column(dataset, *args, **kwargs),
            ),
            intensity_columns=[*dataset.intensity_columns, norm_col_name],
        )

    def get_normalized_column(
        self, dataset: _PsmIntensityDataset, *args, **kwargs
    ) -> _Column:
        """
        Compute normalized intensities for the given dataset.

        Parameters
        ----------
        dataset : PsmIntensityDataset
            The dataset to normalize
        args :
            Positional arguments from the caller
        kwargs :
            Keyword argumentes from the caller

        Returns
        -------
        A :py:class:`pyspark.sql.Column` giving normalized intensities
        """
        raise NotImplementedError("TODO: implement " + self.__name__)
