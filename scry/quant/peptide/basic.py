"""
`scry.quant.peptide.basic` -- simple quantification backend
"""

from typing import (
    overload as _overload,
    Any as _Any,
    Callable as _Callable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

from wheely.mammoth import (
    PsmDataset,
    ConfidenceDataset,
    PsmIntensityDataset,
    PsmIntensityConfidenceDataset,
)
from wheely.mammoth.semantics import (
    NORMALIZED_XIC_AREA as _NORMALIZED_XIC_AREA,
    XIC_AREA as _XIC_AREA,
)

from ..normalization import get_backend as _get_normalization_backend


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
    normalization: _Optional[
        _Union[str, _Callable, _Mapping[str, _Any]]
    ] = None,
) -> PsmIntensityDataset:
    """
    Basic quantification backend

    Requires that quantities have been previously computed and are available in a dataset column.

    To use, run ``scry`` using the following TOML::

        workflow = "v1"

        [peptide_quant]
        backend = "basic"
        sample_column = "…"
        intensity_column = "…"

    or invoke :py:func:`scry.scry` with equivalent parameters.

    Parameters
    ----------
    dset : PsmDataset
    sample_column : str
        The name of the column giving sample IDs
    intensity_column : str
        The name of the column giving PSM intensities
    normalization : str | callable | dict (optional)
        Either a normalization backend name (from :py:class:`scry.quant.normalization.registry`), a normalization
        callable, or a dict with a ``backend`` (name or callable) and optional ``kwargs``.
        If not provided, no normalization will be applied.

    Returns
    -------
    out : PsmIntensityDataset
        A dataset with the specified columns annotated. If ``dset`` is a :py:class:`~wheely.mammoth.ConfidenceDataset`
        then the result will be a :py:class:`ProteinIntensityConfidenceDataset`.
    """
    kwargs = dict(
        sample_column=sample_column,
        intensity_column=intensity_column,
        peptide_column=dset.peptide_column,
        charge_column=dset.charge_column,
        spectrum_columns=dset.spectrum_columns,
        score_columns=dset.score_columns,
        target_column=dset.target_column,
        protein_column=dset.protein_column,
        protein_delim=dset.protein_delim,
        # If the dataset already defines the intensity column's semantics, preserve that annotation
        semantics=dict(
            {
                intensity_column: _XIC_AREA,
            },
            **dset.semantics,
        ),
    )

    if isinstance(dset, ConfidenceDataset):
        kwargs["qvalue_column"] = dset.qvalue_column
        kwargs["errprob_column"] = dset.errprob_column
        kwargs["pi0"] = dset.pi0
        res = PsmIntensityConfidenceDataset(dset.data, **kwargs)
    else:
        res = PsmIntensityDataset(dset.data, **kwargs)

    if normalization is not None:
        if isinstance(normalization, dict):
            norm_backend = normalization.pop("backend")
        else:
            norm_backend = normalization
            normalization = dict()

        if not callable(norm_backend):
            norm_backend = _get_normalization_backend(norm_backend)

        res = norm_backend(res, **normalization)

        res = res.with_data(
            res.data,
            semantics={
                res.intensity_columns: _NORMALIZED_XIC_AREA,
            },
        )

    return res
