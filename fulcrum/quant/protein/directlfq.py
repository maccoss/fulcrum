"""
`fulcrum.quant.protein.directlfq` -- quantification backend that runs DirectLFQ on each protein group
"""

from typing import Union as _Union

from pyspark.sql import Column as _Column

from wheely.mammoth import PsmIntensityDataset
from wheely.mammoth.proteins import ProteinIntensityDataset

from ..rollup import directlfq as _roll_up_directlfq


def quantify_proteins_directlfq(
    dset: PsmIntensityDataset,
    qvalue_threshold: float = None,
    filter_column: _Union[str, _Column] = None,
    rollup_peptides: bool = False,
) -> ProteinIntensityDataset:
    """
    Roll up PSM/precursor/peptide intensities to the protein level using DirectLFQ.

    In typical usage, the dataset should be filtered to give only confident IDs. For
    convenience, you can provide a :py:class:`ConfidenceDataset` and specify a ``qvalue_threshold``
    to use only rows with sufficient confidence, or specify an appropriate ``filter_column``.

    To use, run ``fulcrum`` using the following TOML::

        workflow = "v1"

        [protein_quant]
        backend = "directlfq"
        qvalue_threshold = 0.01

    or invoke :py:func:`fulcrum.fulcrum` with equivalent parameters.

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
    rollup_peptides : bool (optional)
        If ``False`` (default), PSM/precursor intensities will be rolled up directly to the protein level.
        If ``True``, intensities will first be rolled up to the peptide level (by taking the maximum intensity
        across PSMs/precursors for each peptide in each sample), and then peptides will be rolled up to the protein level.
        Note: use of ``rollup_peptides=True`` is *NOT RECOMMENDED*, but was the default behavior in releases prior to
        version 1.7.0 and is preserved for backward compatibility.

    Returns
    -------
    """
    feature_key_columns = [dset.peptide_column]
    if (
        not rollup_peptides
        and getattr(dset, "charge_column", None) is not None
    ):
        feature_key_columns.append(dset.charge_column)

    rolled = _roll_up_directlfq(
        dset,
        entity_key_columns=[dset.protein_column],
        sample_column=dset.sample_column,
        feature_key_columns=feature_key_columns,
        intensity_columns={dset.intensity_column: "directlfq_intensity"},
        preserved_column_reductions={dset.target_column: "max"},
        qvalue_threshold=qvalue_threshold,
        filter_column=filter_column,
    )

    return ProteinIntensityDataset(
        rolled,
        sample_column=dset.sample_column,
        intensity_column="directlfq_intensity",
        protein_column=dset.protein_column,
        protein_delim=dset.protein_delim,
        target_column=dset.target_column,
        score_columns=[],
    )
