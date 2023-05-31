"""
`scry.output.library` -- workflow module that supports transforming sets of PSMs into a format
suitable for use with library search tools.
"""

from typing import (
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import Column as _Column

from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)


def write_library(
    dataset: _PsmDataset,
    location: str,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = 0.01,
):
    """
    Write the given dataset to the given location, formatted for use as a spectral library.

    Filtering -- If the optional `threshold_col` parameter is provided, only rows where this column
    is `True` will be included in the output. If `threshold_col` is not specified but the dataset is
    a `wheely.mammoth.ConfidenceDataset` the optional `qval_thresh` parameter will be used to filter
    PSMs. Otherwise all PSMs in the dataset will be included in the output.

    * TODO: define the source of spectral / RT information

    Libraries are written in a TSV format compatible with DIA-NN and EncyclopeDIA, and suitable for
    conversion to other formats using existing tools. For more information see
    [DIA-NN format documentation](https://github.com/vdemichev/DiaNN#spectral-library-formats).
    Each row represents a single fragment ion in the library.

    Specifically, the following columns are included, in order:

    These columns are the same for each ion in an entry:

    - `ModifiedPeptide` -- a string representation of the peptide and modifications
        TODO: some source datasets may define an incompatible string format, which will be preserved
    - `PrecursorCharge`
    - `PrecursorMz`
    - `Tr_recalibrated` -- The retention time of the ID in an arbitrary scale (possibly all the same
        value, always numeric)

    These columns are specific to each ion in an entry:

    - `ProductMz`
    - `LibraryIntensity` -- relative intensity of the fragment; guaranteed to be numeric and non-negative

    Additional columns that will be written conditionally
    - `QValue` -- _q_-value if the dataset is a `ConfidenceDataset`
    - `IonMobility` -- currently never written

    Currently column names can not be controlled, and are the same regardless of the input dataset
    and its column names, unless noted above.

    Future directions:
    * TODO: add support for other dimensions: e.g. IM

    Parameters
    ----------
    dataset: The dataset
    location: The output location (path or URI)
    threshold_col (str | pyspark.sql.Column; optional): A column (or its name) specifying which
        rows will be included in the resulting library.
    qval_thresh (float; default = 0.01): The largest _q_-value accepted into the library. Ignored if
        the dataset is not a `wheely.mammoth.ConfidenceDataset` or `threshold_col` is specified.

    Returns
    -------
    `None` on success
    """

    # 1. Filter
    psms = _filter_psms(dataset, threshold_col, qval_thresh)

    # 2. Join spectral info
    raise NotImplementedError("TODO")  # TODO

    # 3. Write output
    # TODO

    # 4. Return
    return None


def _filter_psms(
    dataset: _PsmDataset,
    threshold_col: _Optional[_Union[str, _Column]],
    qval_thresh: float,
):
    """
    Return a dataset containing only filtered PSMs, according to the logic described above.

    TODO: tests for this logic
    """

    if threshold_col is None:
        if isinstance(dataset, _ConfidenceDataset):
            if qval_thresh is None:
                # We require a qval_thresh in this case, but we could fall back to no filtering...
                raise ValueError("No qval_thresh specified!")

            return dataset.with_data(
                dataset.data.filter(dataset.qvalues <= qval_thresh)
            )

        # No filtering possible
        return dataset

    return dataset.with_data(dataset.data.filter(threshold_col))
