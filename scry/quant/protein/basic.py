"""
`scry.quant.protein.basic` -- simple quantification backend
"""

from typing import (
    Callable as _Callable,
    Literal as _Literal,
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import (
    functions as _fns,
    Column as _Column,
)

from wheely.mammoth import (
    PsmIntensityDataset,
    ConfidenceDataset,
)
from wheely.mammoth.proteins import (
    ProteinIntensityDataset,
)


def quantify_proteins_basic(
    dset: PsmIntensityDataset,
    qvalue_threshold: float = None,
    filter_column: _Union[str, _Column] = None,
    reduction: _Optional[
        _Union[
            _Callable[[_Column], _Column], _Literal["sum", "max"]  # noqa: F821
        ]
    ] = None,
) -> ProteinIntensityDataset:
    """
    Roll up PSM/precursor/peptide intensities to the protein level.
    This will result in one row per `(dset.sample_column, dset.protein_column)` pair,
    with the intensity computed by `reduction`.

    In typical usage, the dataset should be filtered to give only confident IDs. For
    convenience, you can provide a `ConfidenceDataset` and specify a `qvalue_threshold`
    to use only rows with sufficient confidence.

    IMPORTANT: the dataset's `protein_column` should give protein group identifiers!

    Parameters
    ----------
    dset : PsmIntensityDataset
    qvalue_threshold : float
        If provided, `dset` will be filtered to the given confidence level before rolling up to the protein level;
        in this case the dataset must be a `ConfidenceDataset`. If `None` no q-value filtering will be performed and
        all PSMs will be rolled up. This option can be specified in combination with `filter_column`, in which case
        only rows passing both filters will be rolled up.
    filter_column : str|Column (optional)
        If provided, `dset` will be filtered to only rows with a true value in the specified column before rolling up
        to the protein level. If `None` no filtering will be performed. This option can be specified in combination
        with `qvalue_threshold`, in which case only rows passing both filters will be rolled up.
    reduction : str
        Either "sum" or "max". Default: "sum"

    Returns
    -------
    """
    if reduction is None:
        reduction = "sum"

    # TODO: reduction registry
    if not callable(reduction):
        reduction = {
            "sum": _fns.sum,
            "max": _fns.max,
        }[reduction]

    if qvalue_threshold is not None:
        assert isinstance(
            dset, ConfidenceDataset
        ), "dset must be a ConfidenceDataset if qvalue_threshold is specified"
        dset = dset.with_data(
            dset.data.filter(dset.qvalues <= qvalue_threshold),
        )

    if filter_column is not None:
        dset = dset.with_data(
            dset.data.filter(filter_column),
        )

    return ProteinIntensityDataset(
        dset.data.groupBy(dset.proteins, dset.samples).agg(
            _fns.max(dset.targets).alias(dset.target_column),
            reduction(dset.intensities).alias("intensity"),
        ),
        sample_column=dset.sample_column,
        intensity_column="intensity",
        protein_column=dset.protein_column,
        protein_delim=dset.protein_delim,
        target_column=dset.target_column,
        score_columns=[],
    )
