"""
`scry.quant.peptide.basic` -- simple quantification backend
"""

from typing import (
    overload as _overload,
)

from wheely.mammoth import (
    PsmDataset,
    ConfidenceDataset,
    PsmIntensityDataset,
    PsmIntensityConfidenceDataset,
)


@_overload  # type: ignore[misc]
def quantify_basic(
    dset: ConfidenceDataset,
    sample_column: str,
    intensity_column: str,
) -> PsmIntensityConfidenceDataset: ...


def quantify_basic(
    dset: PsmDataset,
    sample_column: str,
    intensity_column: str,
) -> PsmIntensityDataset:
    """
    Basic quantification backend -- just use the specified columns.

    Parameters
    ----------
    dset : PsmDataset
    sample_column : str
        The name of the column giving sample IDs
    intensity_column : str
        The name of the column giving PSM intensities

    Returns
    -------
    out : PsmIntensityDataset
        A dataset with the specified columns annotated. If `dset` is a `ConfidenceDataset` then the result will
        be a `ProteinIntensityConfidenceDataset`.
    """
    kwargs = dict(
        sample_column=sample_column,
        intensity_column=intensity_column,
        peptide_column=dset.peptide_column,
        spectrum_columns=dset.spectrum_columns,
        score_columns=dset.score_columns,
        target_column=dset.target_column,
        protein_column=dset.protein_column,
        protein_delim=dset.protein_delim,
    )

    if isinstance(dset, ConfidenceDataset):
        kwargs["qvalue_column"] = dset.qvalue_column
        kwargs["errprob_column"] = dset.errprob_column
        kwargs["pi0"] = dset.pi0
        res = PsmIntensityConfidenceDataset(dset.data, **kwargs)
    else:
        res = PsmIntensityDataset(dset.data, **kwargs)

    return res
