"""
`scry.quant.protein.directlfq` -- quantification backend that runs DirectLFQ on each protein group
"""

import logging
from typing import (
    Union as _Union,
)

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

_logger = logging.getLogger(__name__)


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
    rollup_peptides : bool (optional)
        If ``False`` (default), PSM/precursor intensities will be rolled up directly to the protein level.
        If ``True``, intensities will first be rolled up to the peptide level (by taking the maximum intensity
        across PSMs/precursors for each peptide in each sample), and then peptides will be rolled up to the protein level.
        Note: use of ``rollup_peptides=True`` is *NOT RECOMMENDED*, but was the default behavior in releases prior to
        Scry 1.7.0 and is preserved for backward compatibility.

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
    charge_col = (
        None if rollup_peptides else getattr(dset, "charge_column", None)
    )
    samp_col = dset.sample_column
    inten_col = dset.intensity_column
    prot_col = dset.protein_column
    tgt_col = dset.target_column

    # Get the current log level of the directlfq logger from the outer execution context
    # so we can apply it inside the UDF.
    directlfq_log_level = logging.getLogger("directlfq").getEffectiveLevel()

    def estimate_udf(pdf: _pd.DataFrame) -> _pd.DataFrame:
        # Generate ion identifiers if needed
        if charge_col is None:
            ion_col = pep_col
        else:
            ion_col = "__ion"
            pdf[ion_col] = (
                pdf[pep_col].astype(str) + "+" + pdf[charge_col].astype(str)
            )

        # Pivot to wide format: index=peptide/precursor, columns=sample, values=intensity
        wide = pdf.pivot_table(
            index=[prot_col, ion_col],
            columns=samp_col,
            values=inten_col,
            aggfunc="first",
            fill_value=None,  # keep NaN for missing
        )

        # Check if we have any non-zero values before proceeding
        if wide.empty:
            # Return empty dataframe with correct columns
            return _pd.DataFrame(
                columns=[prot_col, samp_col, "directlfq_intensity", tgt_col]
            )

        # Replace zeros with NaN before log transform to avoid log(0)
        wide.replace(0, _np.nan, inplace=True)

        # Also check for negative values which would cause log transformation to fail
        wide_is_neg = wide < 0
        if (wide_is_neg).any().any():
            _logger.warning(
                f"Negative intensity values found for {pdf[prot_col].iloc[0]}"
            )
            wide[wide_is_neg] = _np.nan

        # Check if we have any valid values to process
        if wide.notna().sum().sum() == 0:
            return _pd.DataFrame(
                columns=[prot_col, samp_col, "directlfq_intensity", tgt_col]
            )

        # Import directlfq here (inside the UDF), as we would like to override its logging configuration
        from directlfq import config as _lfq_config

        _lfq_config.setup_logging = lambda *_, **__: ()

        from directlfq.protein_intensity_estimation import (
            estimate_protein_intensities,
        )

        logging.getLogger("directlfq").setLevel(directlfq_log_level)

        # Apply log2 transformation - DirectLFQ expects log2-transformed input
        wide = _np.log2(wide)

        # Name index levels appropriately for DirectLFQ
        wide.index.set_names(
            [_lfq_config.PROTEIN_ID, _lfq_config.QUANT_ID], inplace=True
        )

        # Skip additional work
        _lfq_config.set_compile_normalized_ion_table(False)

        # Call the estimation function
        protein_df, _ = estimate_protein_intensities(
            wide, min_nonan=1, num_samples_quadratic=10, num_cores=1
        )

        # If we got an empty result, return empty dataframe
        if protein_df.empty:
            return _pd.DataFrame(
                columns=[prot_col, samp_col, "directlfq_intensity", tgt_col]
            )

        protein_df.rename(
            columns={_lfq_config.PROTEIN_ID: prot_col}, inplace=True
        )

        # Convert wide to long format efficiently with stack()
        protein_long = (
            protein_df.set_index(prot_col)
            .stack()
            .reset_index()
            .rename(columns={"level_1": samp_col, 0: "directlfq_intensity"})
        )

        # Add target column (max for group) -- any group with a target peptide is a target protein
        protein_long[tgt_col] = pdf[tgt_col].max()

        return protein_long

    df_agg = (
        dset.data.groupBy(dset.protein_column)
        .applyInPandas(estimate_udf, schema)
        .cache()  # cache to avoid recomputation
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
