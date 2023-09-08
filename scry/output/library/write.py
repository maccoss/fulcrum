"""
`scry.output.library.write` -- implements overall workflow module
"""

import logging as _logging
import re as _re
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
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
from wheely.mammoth.spectra import (
    SpectraDataset as _LibSpectra,
)
from wheely.mammoth.spectra.utils import (
    peaklist_to_pairs as _peaklist_to_pairs,
)

from .spectra import get_backend as _get_spectra_backend

_logger = _logging.getLogger(__name__)


def write_library(
    dataset: _PsmDataset,
    spectra_backend: _Union[str, _Callable],
    location: _Optional[str] = None,
    output_location: _Optional[str] = None,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    peptide_normalizer: _Optional[_Dict[str, _Any]] = None,
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

    - `ModifiedPeptide` -- a string representation of the peptide and modifications. This will be
        taken from the input dataset's `peptide_column` then (optionally) normalized by the
        specified `peptide_normalizer`.
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
    location: The output location (path or URI)
    output_location: DEPRECATED synonym for `location`
    spectra_backend (str | callable): The backend implementation used to look up library spectral
        information for each supplied PSM.
    threshold_col (str | pyspark.sql.Column; optional): A column (or its name) specifying which
        rows will be included in the resulting library.
    qval_thresh (float; default = 0.01): The largest _q_-value accepted into the library. Ignored if
        the dataset is not a `wheely.mammoth.ConfidenceDataset` or `threshold_col` is specified.
    peptide_normalizer (dict; optional): A dict whose `backend` (a `callable`) will be called to
        normalize each `ModifiedPeptide` value (from `dataset.peptide_column`). Any dict entries
        other than `backend` will be passed to the callable as keyword arguments. If unspecified
        or `None` a generic normalizer will be used, which provides a "best-effort" normalization
        to DIA-NN like Unimod format (e.g. `C(Unimod:4)`). A false-y value for `peptide_normalizer`
        or `peptide_normalizer["backend"]` will disable normalization.
        TODO: support a registry of available backend normalizers and permit `backend` to be a str
    **kwargs: Any additional keyword arguments are passed to the spectra_backend callable.
    Returns
    -------
    A PySpark DataFrame with the same contents as the output library.
    """
    if not location:
        location = output_location

    if not spectra_backend:
        raise ValueError("spectra_backend may not be None!")

    _spectra_backend: _Callable
    if callable(spectra_backend):
        _spectra_backend = spectra_backend
    else:
        _spectra_backend = _get_spectra_backend(spectra_backend)

    # 1. Filter / normalize
    psms = _filter_psms(dataset, threshold_col, qval_thresh)

    if _logger.isEnabledFor(_logging.INFO):
        n_filt = psms.data.count()
        _logger.info("Building library from %d PSMs (after filtering)", n_filt)
        assert n_filt >= 0
    else:
        assert not psms.data.isEmpty()

    if peptide_normalizer is None:
        _logger.debug(
            "Normalizing peptide sequences and mods with default normalizer"
        )
        norm_psms = _normalize_peptides(psms)
    elif peptide_normalizer:
        _logger.debug(
            "Normalizing peptide sequences and mods with: %s",
            peptide_normalizer,
        )
        norm_psms = _normalize_peptides(psms, **peptide_normalizer)
    else:
        _logger.debug("Skipping normalization of peptide sequences and mods")
        norm_psms = psms

    # 2. Join spectral info
    spectra: _LibSpectra = _spectra_backend(norm_psms, **kwargs)

    if _logger.isEnabledFor(_logging.INFO):
        n_spec = spectra.data.count()
        _logger.info("Found %d spectra", n_spec)
        assert n_spec > 0
    else:
        assert not spectra.data.isEmpty()

    assert (
        dataset.spectrum_columns == spectra.spectrum_columns
    ), f"Unsupported: differing spectrum IDs! PSMs had {dataset.spectrum_columns} but spectra had {spectra.spectrum_columns}"

    joined_df: _DataFrame = dataset.data.alias("psms").join(
        spectra.data.alias("spectra"), on=dataset.spectrum_columns
    )

    if _logger.isEnabledFor(_logging.INFO):
        n_join = joined_df.count()
        _logger.info("Will write %d PSMs to library (after join)", n_join)
        assert n_join > 0
    else:
        assert not joined_df.isEmpty()

    # Selecting this "explodes" the peaklist into one row per fragment peak
    peak = _peaklist_to_pairs(
        _fns.col("spectra." + spectra.peaklist_column)
    ).alias("__peak")

    # 3. Build, name, and select columns
    output = (
        joined_df.select(
            # TODO: clarify / document this use of `peptide_column`
            _fns.col("psms." + psms.peptide_column).alias("ModifiedPeptide"),
            _fns.col("spectra." + spectra.charge_column).alias(
                "PrecursorCharge"
            ),
            _fns.col("spectra." + spectra.mz_column).alias("PrecursorMz"),
            _fns.col("spectra." + spectra.rt_column).alias("Tr_recalibrated"),
            # We must select this up front, it will be aliased into the correct position below
            *(
                [_fns.col("psms." + dataset.qvalue_column).alias("__qvalue")]
                if isinstance(dataset, _ConfidenceDataset)
                else []
            ),
            peak,
        )
        .select(
            "*",
            _fns.col("__peak").getItem(0).alias("ProductMz"),
            _fns.col("__peak").getItem(1).alias("LibraryIntensity"),
        )
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
    if location:
        # Repartition to get a single TSV file; this will still produce
        # an output folder with Spark metadata.
        # TODO: consider using .toPandas() and writing to the location as a single file, given we're assuming it's small enough for one file anyway
        output.repartition(1).write.csv(
            location,
            sep="\t",
            header=True,
        )

    # 5. Return
    return output


def _filter_psms(
    dataset: _PsmDataset,
    threshold_col: _Optional[_Union[str, _Column]],
    qval_thresh: _Optional[float],
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


#: Pattern used to find modifications that will be string-substituted
_mod_heuristic_pattern = _re.compile(r"([A-Z])(\[.+?\]|\(.+?\))")


def _normalize_mod_heuristic(match: _re.Match) -> str:
    """
    Default "best-effort" modification normalizer, based on heuristics that address only common use
    cases.

    Parameters
    ----------
    match: A match object, corresponding to the `_mod_heuristic_pattern`.

    Returns
    -------
    A reformatted string meant to be compatible (but not guaranteed to be!) with DIA-NN.
    """
    residue = match.group(1)

    # Ignore captured brackets
    mod = match.group(2)[1:-1]

    try:
        delta = float(mod)
    except:
        # Not a numeric mod; give up!
        pass
    else:
        # Heuristic lookup
        if residue.upper() == "C" and round(delta) == 57:
            return residue + "(Unimod:4)"
        if residue.upper() == "M" and round(delta) == 16:
            return residue + "(Unimod:35)"

    # Give up; return the originally-captured (sub)string
    return match.group(0)


def _normalize_peptide_heuristic(seq):
    """
    Default "best-effort" peptide normalizer, based on heuristics that address only common use cases.

    Parameters
    ----------
    seq: A peptide sequence string, including mods in a "typical" format.

    Returns
    -------
    A reformatted string meant to be compatible (but not guaranteed to be!) with DIA-NN.
    """
    return _mod_heuristic_pattern.sub(
        string=seq, repl=_normalize_mod_heuristic
    )


def _normalize_peptides(
    psms: _PsmDataset, backend: _Optional[_Callable] = None, **kwargs
) -> _PsmDataset:
    """
    Normalize each value from `dataset.peptide_column`.

    peptide_normalizer (dict; optional): A dict whose `backend` (a `callable`) will be called to
        Any dict entries
        other than `backend` will be passed to the callable as keyword arguments.
    Parameters
    ----------
    psms: The dataset that will be normalized
    backend: A callable that will be passed each peptide value, returning the normalized value.
        If unspecified or `None` a generic normalizer will be used, which provides a "best-effort"
        normalization to DIA-NN like Unimod format (e.g. `C(Unimod:4)`). Any other false-y value
        will disable normalization.
        TODO: support a registry of available backend normalizers and permit `backend` to be a str
    kwargs: Any keyword arguments will be passed to each invocation of `backend`.

    Returns
    -------
    A PSM dataset with the `peptide_column` values normalized by the given backend.
    """
    if backend is None:
        _backend = _normalize_peptide_heuristic
    elif not backend:  # type: ignore[truthy-function]
        return psms

    assert callable(_backend)

    orig_pep_col = "__peptide_orig"
    return psms.with_data(
        psms.data.withColumnRenamed(psms.peptide_column, orig_pep_col)
        .withColumn(
            psms.peptide_column,
            _fns.udf(lambda seq: _backend(seq, **kwargs))(
                _fns.col(orig_pep_col)
            ),
        )
        .drop(orig_pep_col)
    )
