"""
`fulcrum.quant.protein.basic` -- simple quantification backend
"""

from typing import (
    Callable as _Callable,
    Literal as _Literal,
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import Column as _Column

from wheely.mammoth import (
    PsmIntensityDataset,
)
from wheely.mammoth.proteins import (
    ProteinIntensityDataset,
)

from ..rollup import basic as _roll_up_basic
from ..rollup.utils import (
    resolve_rollup_output_intensity_columns,
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
    This will result in one row per ``(dset.sample_column, dset.protein_column)`` pair,
    with the intensity computed by ``reduction``.

    In typical usage, the dataset should be filtered to give only confident IDs. For
    convenience, you can provide a :py:class:`ConfidenceDataset` and specify a ``qvalue_threshold``
    to use only rows with sufficient confidence, or specify an appropriate ``filter_column``.

    To use, run ``fulcrum`` using the following TOML::

        workflow = "v1"

        [protein_quant]
        backend = "basic"
        qvalue_threshold = 0.01

    or invoke :py:func:`fulcrum.fulcrum` with equivalent parameters.

    Note: when multiple intensity columns are present in the input dataset (for example, raw and normalized intensities)
    they will all be rolled up to separate protein-level intensity columns. This can result in a protein-level
    "normalized intensity" column which is calculated by rolling up normalized intensities, rather than applying a
    separate normalization step to rolled-up intensities, as shown below::

        raw precursor intensity -> rollup -> protein-level intensity
        |
        v
        normalized precursor intensity -> rollup -> protein-level normalized intensity

    Parameters
    ----------
    dset : PsmIntensityDataset
        IMPORTANT: the dataset's ``protein_column`` should give **protein group identifiers**!
    qvalue_threshold : float
        If provided, ``dset`` will be filtered to the given confidence level before rolling up to the protein level;
        in this case the dataset must be a :py:class:`ConfidenceDataset`. If ``None`` no q-value filtering will be
        performed and all PSMs will be rolled up. This option can be specified in combination with ``filter_column``,
        in which case only rows passing both filters will be rolled up.
    filter_column : str|Column (optional)
        If provided, ``dset`` will be filtered to only rows with a true value in the specified column before rolling up
        to the protein level. If ``None`` no filtering will be performed. This option can be specified in combination
        with ``qvalue_threshold``, in which case only rows passing both filters will be rolled up.
    reduction : str
        Either "sum" or "max". Default: "sum"

    Returns
    -------
    """
    if reduction is None:
        reduction = "sum"

    (
        output_intensity_columns,
        intensity_column,
        intensity_semantics,
    ) = resolve_rollup_output_intensity_columns(dset)

    rolled = _roll_up_basic(
        dset,
        entity_key_columns=[dset.protein_column],
        sample_column=dset.sample_column,
        feature_key_columns=None,
        intensity_columns=output_intensity_columns,
        intensity_reduction=reduction,
        preserved_column_reductions={dset.target_column: "max"},
        qvalue_threshold=qvalue_threshold,
        filter_column=filter_column,
    )

    return ProteinIntensityDataset(
        rolled,
        sample_column=dset.sample_column,
        intensity_column=intensity_column,
        intensity_columns=list(output_intensity_columns.values()),
        protein_column=dset.protein_column,
        protein_delim=dset.protein_delim,
        target_column=dset.target_column,
        score_columns=[],
        semantics={
            **{
                column_name: dset.semantics[column_name]
                for column_name in (
                    dset.sample_column,
                    dset.protein_column,
                    dset.target_column,
                )
                if column_name in dset.semantics
            },
            **intensity_semantics,
        },
    )
