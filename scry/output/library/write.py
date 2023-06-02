"""
`scry.output.library.write` -- implements overall workflow module
"""

from typing import (
    Callable as _Callable,
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import (
    Column as _Column,
    DataFrame as _DataFrame,
    functions as _fns,
)

from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)

from .spectra import (
    LibrarySpectraDataset as _LibSpectra,
    get_backend as _get_spectra_backend,
)
from .spectra.utils import (
    peaklist_to_pairs,
)


def write_library(
    dataset: _PsmDataset,
    spectra_backend: _Union[str, _Callable],
    output_location: _Optional[str] = None,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    **kwargs,
) -> _DataFrame:
    """
    Write the given dataset to the given location, formatted for use as a spectral library.

    Filtering -- If the optional `threshold_col` parameter is provided, only rows where this column
    is `True` will be included in the output. If `threshold_col` is not specified but the dataset is
    a `wheely.mammoth.ConfidenceDataset` the optional `qval_thresh` parameter will be used to filter
    PSMs. Otherwise all PSMs in the dataset will be included in the output.

    Spectral information -- This module is meant to consume scored and filtered sets of PSMs, that
    may not necessarily include the necessary spectral information for creating a library.
    Retrieval of this information is implemented by a pluggable backend implementation capable of
    fetching the precursor- and fragment-level spectral information for PSMs in a filtered dataset.

    Output -- Libraries are written in a TSV format compatible with DIA-NN and EncyclopeDIA, and
    suitable for conversion to other formats using existing tools. For more information see
    [DIA-NN format documentation](https://github.com/vdemichev/DiaNN#spectral-library-formats).
    Each row represents a single fragment ion in the library. If `output_location` is truthy
    the library will be written to that location. In all cases, the same dataset is returned by
    this function as a PySpark DataFrame.

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

    Additional columns that will be written conditionally:

    - `QValue` -- _q_-value if the dataset is a `ConfidenceDataset`
    - `IonMobility` -- currently never written

    Currently column names can not be controlled, and are the same regardless of the input dataset
    and its column names, unless noted above.

    Future directions:
    * Add support for other dimensions: e.g. IM
    * Add support for customizing output: e.g. column names, Spark output kwargs, etc.

    Parameters
    ----------
    dataset: The dataset
    output_location: The output location (path or URI)
    spectra_backend (str | callable): The backend implementation used to look up library spectral
        information for each supplied PSM.
    threshold_col (str | pyspark.sql.Column; optional): A column (or its name) specifying which
        rows will be included in the resulting library.
    qval_thresh (float; default = 0.01): The largest _q_-value accepted into the library. Ignored if
        the dataset is not a `wheely.mammoth.ConfidenceDataset` or `threshold_col` is specified.
    **kwargs: Any additional keyword arguments are passed to the spectra_backend callable.

    Returns
    -------
    A PySpark DataFrame with the same contents as the output library.
    """
    if not spectra_backend:
        raise ValueError("spectra_backend may not be None!")

    if not callable(spectra_backend):
        spectra_backend = _get_spectra_backend(spectra_backend)

    # 1. Filter
    psms = _filter_psms(dataset, threshold_col, qval_thresh)

    # 2. Join spectral info

    spectra: _LibSpectra = spectra_backend(psms, **kwargs)

    assert (
        dataset.spectrum_columns == spectra.spectrum_columns
    ), f"Unsupported: differing spectrum IDs! PSMs had {dataset.spectrum_columns} but spectra had {spectra.spectrum_columns}"

    joined_frags: _DataFrame = dataset.data.join(
        spectra.data, on=dataset.spectrum_columns
    )

    # Selecting this "explodes" the peaklist into one row per fragment peak
    pairs = peaklist_to_pairs(spectra.peaklists)

    # 3. Build, name, and select columns
    output = (
        joined_frags.select(
            # TODO: clarify / document this use of `peptide_column`
            _fns.col(psms.peptide_column).alias("ModifiedPeptide"),
            _fns.col(spectra.charge_column).alias("PrecursorCharge"),
            _fns.col(spectra.mz_column).alias("PrecursorMz"),
            _fns.col(spectra.rt_column).alias("Tr_recalibrated"),
            # We must select this up front, it will be aliased into the correct position below
            *(
                [_fns.col(dataset.qvalue_column).alias("__qvalue")]
                if isinstance(dataset, _ConfidenceDataset)
                else []
            ),
            pairs.alias("__peak"),
        )
        .withColumn("ProductMz", _fns.col("__peak").getItem(0))
        .withColumn("LibraryIntensity", _fns.col("__peak").getItem(1))
        .drop("__peak")
    )

    # Conditionally append column
    if isinstance(dataset, _ConfidenceDataset):
        output = output.withColumn(
            # Note: We take col name from _dataset_, so we only assume the column is present
            # just in case _filter_psms / with_data returns a different type of dataset.
            "QValue",
            _fns.col("__qvalue"),
        ).drop("__qvalue")

    # 4. Write output
    if output_location:
        # Repartition to get a single TSV file; this will still produce
        # an output folder with Spark metadata.
        # TODO: consider using .toPandas() and writing to the location as a single file, given we're assuming it's small enough for one file anyway
        output.repartition(1).write.csv(
            output_location,
            sep="\t",
            header=True,
        )

    # 5. Return
    return output


def _filter_psms(
    dataset: _PsmDataset,
    threshold_col: _Optional[_Union[str, _Column]],
    qval_thresh: float,
) -> _PsmDataset:
    """
    Return a dataset containing only filtered PSMs, according to the logic described above.
    """

    if threshold_col is None:
        if isinstance(dataset, _ConfidenceDataset):
            if qval_thresh is not None:
                return dataset.with_data(
                    dataset.data.filter(dataset.qvalues <= qval_thresh)
                )
            else:
                # Fall through
                pass

        # No filtering possible
        return dataset

    return dataset.with_data(dataset.data.filter(threshold_col))
