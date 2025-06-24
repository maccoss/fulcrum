"""
`scry.quant.protein.directlfq` -- quantification backend that runs DirectLFQ on each protein group
"""

from typing import (
    Union as _Union,
)

from directlfq import config as _lfq_config
from directlfq.protein_intensity_estimation import estimate_protein_intensities
import numpy as _np
import pandas as _pd
from pyspark.sql import (
    functions as _fns,
    Column as _Column,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

from wheely.mammoth import (
    PsmIntensityDataset,
    ConfidenceDataset,
)
from wheely.mammoth.proteins import (
    ProteinIntensityDataset,
)


def quantify_proteins_directlfq(
    dset: PsmIntensityDataset,
    qvalue_threshold: float = None,
    filter_column: _Union[str, _Column] = None,
) -> ProteinIntensityDataset:
    """
    Roll up PSM/precursor/peptide intensities to the protein level using DirectLFQ.

    In typical usage, the dataset should be filtered to give only confident IDs. For
    convenience, you can provide a :py:class:`ConfidenceDataset` and specify a ``qvalue_threshold``
    to use only rows with sufficient confidence, or specify an appropriate ``filter_column``.

    To use, run ``scry`` using the following TOML::

        workflow = "v1"

        [protein_quant]
        backend = "directlfq"
        qvalue_threshold = 0.01

    or invoke :py:func:`scry.scry` with equivalent parameters.

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

    Returns
    -------
    """
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

    # Define the schema for the output DataFrame
    schema = StructType(
        [
            StructField(dset.protein_column, StringType()),
            StructField(dset.sample_column, StringType()),
            StructField("directlfq_intensity", DoubleType()),
            StructField(dset.target_column, BooleanType()),
        ]
    )

    # Get the column names from the dataset; these will be used in the UDF
    pep_col = dset.peptide_column
    samp_col = dset.sample_column
    inten_col = dset.intensity_column
    prot_col = dset.protein_column
    tgt_col = dset.target_column

    def estimate_udf(pdf: _pd.DataFrame) -> _pd.DataFrame:
        # Pivot to wide format: index=peptide/precursor, columns=sample, values=intensity
        wide = pdf.pivot_table(
            index=[prot_col, pep_col],
            columns=samp_col,
            values=inten_col,
            aggfunc="first",
            fill_value=None,  # keep NaN for missing
        )

        # Name index levels appropriately for DirectLFQ
        wide.index.set_names(
            [_lfq_config.PROTEIN_ID, _lfq_config.PROTEIN_ID], inplace=True
        )

        # Replace zeros with NaN
        wide.replace(0, _np.nan, inplace=True)

        # Skip additional work
        _lfq_config.set_compile_normalized_ion_table(False)

        # Call the estimation function
        protein_df, _ = estimate_protein_intensities(
            wide, min_nonan=1, num_samples_quadratic=10, num_cores=1
        )

        protein_df.rename(
            columns={_lfq_config.PROTEIN_ID: prot_col}, inplace=True
        )

        # Convert wide to long format using melt
        protein_long = _pd.melt(
            protein_df,
            id_vars=[prot_col],
            var_name=samp_col,
            value_name="directlfq_intensity",
        )

        # Add target column (max for group) -- any group with a target peptide is a target protein
        protein_long[tgt_col] = pdf[tgt_col].max()

        return protein_long

    df_agg = dset.data.groupBy(dset.protein_column).applyInPandas(
        estimate_udf, schema
    )

    return ProteinIntensityDataset(
        df_agg,
        sample_column=dset.sample_column,
        intensity_column="directlfq_intensity",
        protein_column=dset.protein_column,
        protein_delim=dset.protein_delim,
        target_column=dset.target_column,
        score_columns=[],
    )
