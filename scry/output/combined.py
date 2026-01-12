"""
`scry.output.combined`: combined peptide and protein output
"""

import logging as _logging
from typing import (
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import (
    Column as _Column,
    functions as _fns,
)

from wheely.mammoth import PsmDataset as _PsmDataset
from wheely.mammoth.proteins import ProteinDataset as _ProteinDataset
from wheely.mammoth.semantics import (
    NORMALIZED_RT_IN_SECONDS as _NORMALIZED_RT_IN_SECONDS,
    NORMALIZED_XIC_AREA,
    PEPTIDE_ERRPROB as _PEPTIDE_ERRPROB,
    PEPTIDE_QVALUE as _PEPTIDE_QVALUE,
    PEPTIDOFORM_SEQUENCE,
    PRECURSOR_ERRPROB as _PRECURSOR_ERRPROB,
    PRECURSOR_QVALUE as _PRECURSOR_QVALUE,
    PROTEIN_GROUP_ERRPROB as _PROTEIN_GROUP_ERRPROB,
    PROTEIN_GROUP_QVALUE as _PROTEIN_GROUP_QVALUE,
    PROTEOTYPIC_PEPTIDE as _PROTEOTYPIC_PEPTIDE,
    PSM_ERRPROB as _PSM_ERRPROB,
    PSM_QVALUE as _PSM_QVALUE,
    RT_IN_SECONDS as _RT_IN_SECONDS,
    RT_START_IN_SECONDS as _RT_START_IN_SECONDS,
    RT_STOP_IN_SECONDS as _RT_STOP_IN_SECONDS,
    SCAN_NUMBER as _SCAN_NUMBER,
    STRIPPED_SEQUENCE,
    THEORETICAL_MONO_MASS as _THEORETICAL_MONO_MASS,
    THEORETICAL_PRECURSOR_MZ as _THEORETICAL_PRECURSOR_MZ,
    XIC_AREA,
)

from .util import filter_psms

_logger = _logging.getLogger(__name__)


def _get_column_by_semantic(dataset, semantic, prefix=None):
    """
    Attempt to get a column from the dataset by its semantic.
    Returns None if the semantic is not found or dataset doesn't support semantics.
    """
    try:
        col_name = dataset.get_by_semantics(semantic, require_unique=False)
        return f"{prefix}.{col_name}" if prefix else col_name
    except (ValueError, AttributeError):
        return None


def write_combined(
    peptides: _PsmDataset,
    proteins: _Optional[_ProteinDataset] = None,
    location: str = None,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    peptide_kwargs: _Optional[dict] = None,
    protein_kwargs: _Optional[dict] = None,
    **kwargs,
):
    """
    Write combined peptide and protein information to Parquet.
    Joins peptide and protein datasets on ``protein_column`` and ``sample_column``,
    then writes aliased columns in a standard format.

    This output backend provides a combined peptide-and-protein report similar to that of DIA-NN. Key columns are
    named consistently in this report, regardless of the underlying names provided by the backend implementations.
    Additional peptide and protein score columns are included in the output to provide additional scoring information.

    Included columns:

    - ``Run`` -- a string identifying the sample for each identification
    - ``Protein.Group`` -- delimiter-separated protein identifiers for the PSM's assigned protein group
    - ``Modified.Sequence`` -- the string representation of the modified peptide sequence
    - ``Stripped.Sequence`` -- the string representation of the unmodified peptide sequence
    - ``Peptide.ID`` -- the unique identifier for this modified peptide sequence (if different from ``Modified.Sequence``)
    - ``Proteotypic`` -- if the peptide is proteotypic (uniquely maps to the protein group)
    - ``Decoy`` -- whether the PSM is from a decoy peptide
    - ``Precursor.Charge``
    - ``Precursor.Mz`` -- theoretical precursor m/z
    - ``Peptide.Mass`` -- theoretical uncharged monoisotopic mass of the peptide
    - ``MS2.Scan`` -- the scan number of the identified peak apex MS2 scan
    - ``RT`` -- the retention time of identified peak apex
    - ``RT.Start`` -- the starting point of the identified peak RT bounds
    - ``RT.Stop`` -- the ending point of the identified peak RT bound
    - ``iRT`` -- the empirical retention time of the identified peak, transformed into library RT space
    - ``Q.Value`` -- PSM-level _q_-value estimate
    - ``PEP`` -- PSM-level posterior error probability estimate
    - ``Global.Precursor.Q.Value`` -- precursor-level _q_-value estimate in the global context
    - ``Global.Precursor.PEP`` -- precursor-level posterior error probability estimate in the global context
    - ``Global.Peptide.Q.Value`` -- precursor-level _q_-value estimate in the global context
    - ``Global.Peptide.PEP`` -- precursor-level posterior error probability estimate in the global context
    - ``Global.PG.Q.Value`` -- protein group-level _q_-value estimate in the global context
    - ``Global.PG.PEP`` -- protein group-level posterior error probability estimate in the global context
    - ``Precursor.Quantity`` -- Raw PSM intensity (Note: for some quant backends this may be normalized PSM intensity,
                                in which case a log message may include more information about its content)
    - ``Precursor.Normalised`` -- Normalized PSM intensity
    - ``PG.Quantity`` -- rolled-up protein group quantity
    - ``*`` -- additional columns give PSM-level identification scores
    - ``PG.*`` -- additional columns with the ``PG`` prefix give protein group-level identification scores

    **Note:** Some of these columns will only be included if they are computed by the workflow and selected backends.
    For example, in common usage only one of ``Global.Precursor.Q.Value`` and ``Global.Peptide.Q.Value`` will be
    reported, depending on the configured ``cortado.pep_fdr_type``

    **Filtering** -- The ``qval_thresh`` and ``include_decoys`` parameters allow convenient filtering of output
    PSMs or proteins.

    For more sophisticated filtering, the optional ``threshold_col`` parameter includes only rows where this column
    is ``True`` in the output. When ``threshold_col`` is specified the ``qval_thresh`` and ``include_decoys`` parameters
    will be ignored.

    Parameters
    ----------
    peptides : PsmDataset
        The peptide dataset to write
    proteins : ProteinDataset
        The protein dataset to join with
    location : str
        A string specifying where the results should be written
    threshold_col : str | pyspark.sql.Column
        (Optional) A column (or its name) specifying which rows will be included in the output
    qval_thresh : float
        (Optional) The largest *q*-value accepted into the output. Ignored if the dataset is not a
        ``wheely.mammoth.ConfidenceDataset`` or ``threshold_col`` is specified.
    include_decoys : bool
        (Optional) If true, include decoy PSMs in the output. Ignored if ``threshold_col`` is specified. Default: ``False``
    peptide_kwargs : dict
        Ignored
    protein_kwargs : dict
        Ignored
    kwargs :
        additional keyword arguments to pass to ``pyspark.sql.DataFrameWrite.parquet``.
        Defaults: ``{"mode": "errorifexists"}``
    """
    if proteins is None:
        raise ValueError("proteins dataset is required for combined output")
    if peptide_kwargs:
        _logger.warning("peptide_kwargs will be ignored: %s", peptide_kwargs)
    if protein_kwargs:
        _logger.warning("protein_kwargs will be ignored: %s", protein_kwargs)

    pep_filt = filter_psms(
        peptides,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
    )

    prot_filt = filter_psms(
        proteins,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
    )

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Will write results for %d peptide rows…", pep_filt.data.count()
        )
        _logger.info(
            "Will write results for %d protein rows…", prot_filt.data.count()
        )

    sample_col = getattr(peptides, "sample_column", None)
    if sample_col is None:
        raise ValueError("peptides dataset must have sample_column")

    prot_sample_col = getattr(proteins, "sample_column", None)
    if prot_sample_col is None:
        raise ValueError("proteins dataset must have sample_column")

    pep_df = pep_filt.data
    prot_df = prot_filt.data

    # Join on protein_column and sample_column
    join_condition = (
        _fns.col(f"pep.{peptides.protein_column}")
        == _fns.col(f"prot.{proteins.protein_column}")
    ) & (_fns.col(f"pep.{sample_col}") == _fns.col(f"prot.{prot_sample_col}"))
    joined = pep_df.alias("pep").join(
        prot_df.alias("prot"), join_condition, "inner"
    )

    # Build output columns
    output_cols = {}

    # Track which columns of the joined DataFrame are used
    input_cols = set()

    # Run: from sample_column
    output_cols["Run"] = _fns.col(f"pep.{sample_col}")
    input_cols.add(f"pep.{sample_col}")

    # Protein.Group: from protein_column
    # TODO: use semantics to resolve grouped vs all proteins
    output_cols["Protein.Group"] = _fns.col(f"pep.{peptides.protein_column}")
    input_cols.add(f"pep.{peptides.protein_column}")

    # Peptide Sequence: we have three possible sources of information:
    # - peptide_column
    # - peptidoform sequence semantic
    # - stripped sequence semantic
    #
    # We want to prioritize the semantics to ensure the column names
    # are semantically meaningful, but should always include the peptide_column
    # because of its importance in the pipeline.
    # Thus, we check the semantics first. If the peptide_column does not have
    # either expected semantic, we either assume it's a modified sequence, or
    # we call it "Peptide.ID"

    mod_seq_col = _get_column_by_semantic(
        peptides, PEPTIDOFORM_SEQUENCE, "pep"
    )
    if mod_seq_col is not None:
        output_cols["Modified.Sequence"] = mod_seq_col
        input_cols.add(mod_seq_col)

    strip_seq_col = _get_column_by_semantic(peptides, STRIPPED_SEQUENCE, "pep")
    if strip_seq_col is not None:
        output_cols["Stripped.Sequence"] = strip_seq_col
        input_cols.add(strip_seq_col)

    pep_col = f"pep.{peptides.peptide_column}"
    if pep_col not in {mod_seq_col, strip_seq_col}:
        # peptide_column was not already included via semantics
        if mod_seq_col is None:
            # Assume peptide_column is modified sequence
            output_cols["Modified.Sequence"] = _fns.col(pep_col)
            input_cols.add(pep_col)
        else:
            output_cols["Peptide.ID"] = _fns.col(pep_col)
            input_cols.add(pep_col)

    # Modified.Sequence: from peptide_column
    output_cols["Modified.Sequence"] = _fns.col(
        f"pep.{peptides.peptide_column}"
    )
    input_cols.add(f"pep.{peptides.peptide_column}")

    # Proteotypic: from semantic, if available
    proteotypic_col = _get_column_by_semantic(
        peptides, _PROTEOTYPIC_PEPTIDE, "pep"
    )
    if proteotypic_col is not None:
        output_cols["Proteotypic"] = _fns.col(proteotypic_col)
        input_cols.add(proteotypic_col)

    # Decoy: from is_decoy_column
    target_column = f"pep.{peptides.target_column}"
    output_cols["Decoy"] = ~_fns.col(
        target_column
    )  # important: invert boolean sense: target -> decoy
    input_cols.add(target_column)

    # Precursor.Charge: from charge_column, if available
    if peptides.charge_column is not None:
        output_cols["Precursor.Charge"] = _fns.col(
            f"pep.{peptides.charge_column}"
        )
        input_cols.add(f"pep.{peptides.charge_column}")

    # Precursor m/z: from m/z semantic, or fall back to mz_column if available
    mz_col = _get_column_by_semantic(
        peptides, _THEORETICAL_PRECURSOR_MZ, "pep"
    )
    if mz_col is None:
        mz_col = getattr(peptides, "mz_column", None)
        if mz_col is not None:
            mz_col = f"pep.{mz_col}"
    if mz_col is not None:
        output_cols["Precursor.Mz"] = _fns.col(mz_col)
        input_cols.add(mz_col)

    # Precursor Mass: from theoretical mass semantic, if available
    mass_col = _get_column_by_semantic(peptides, _THEORETICAL_MONO_MASS, "pep")
    if mass_col is not None:
        output_cols["Peptide.Mass"] = _fns.col(mass_col)
        input_cols.add(mass_col)

    # Scan Number: from semantic, if available
    scan_col = _get_column_by_semantic(peptides, _SCAN_NUMBER, "pep")
    if scan_col is not None:
        output_cols["MS2.Scan"] = _fns.col(scan_col)
        input_cols.add(scan_col)

    # RT: from RT semantic, or fall back to rt_column if available
    rt_col = _get_column_by_semantic(peptides, _RT_IN_SECONDS, "pep")
    if rt_col is None:
        rt_col = getattr(peptides, "rt_column", None)
        if rt_col is not None:
            rt_col = f"pep.{rt_col}"
    if rt_col is not None:
        output_cols["RT"] = _fns.col(rt_col)
        input_cols.add(rt_col)

    # Start RT: from semantic, if available
    rt_start_col = _get_column_by_semantic(
        peptides, _RT_START_IN_SECONDS, "pep"
    )
    if rt_start_col is not None:
        output_cols["RT.Start"] = _fns.col(rt_start_col)
        input_cols.add(rt_start_col)

    # Stop RT: from semantic, if available
    rt_stop_col = _get_column_by_semantic(peptides, _RT_STOP_IN_SECONDS, "pep")
    if rt_stop_col is not None:
        output_cols["RT.Stop"] = _fns.col(rt_stop_col)
        input_cols.add(rt_stop_col)

    # iRT: from iRT semantic
    irt_col = _get_column_by_semantic(
        peptides, _NORMALIZED_RT_IN_SECONDS, "pep"
    )
    if irt_col is not None:
        output_cols["iRT"] = _fns.col(irt_col)
        input_cols.add(irt_col)

    # Q.Value: from PSM-level q-value semantic, if available
    psm_qval_col = _get_column_by_semantic(peptides, _PSM_QVALUE, "pep")
    if psm_qval_col is not None:
        output_cols["Q.Value"] = _fns.col(psm_qval_col)
        input_cols.add(psm_qval_col)

    # PEP: from PSM-level errprob semantic, if available, falling back to errprob_column
    errprob_col = _get_column_by_semantic(peptides, _PSM_ERRPROB, "pep")
    if errprob_col is None:
        errprob_col = getattr(peptides, "errprob_column", "pep")
        if errprob_col is not None:
            errprob_col = f"pep.{errprob_col}"
    if errprob_col is not None:
        output_cols["PEP"] = _fns.col(errprob_col)
        input_cols.add(errprob_col)

    # Global.Precursor.Q.Value: from precursor-level q-value semantic, if available
    # TODO: semantics do not clarify global vs local; assume global for now
    precursor_qval_col = _get_column_by_semantic(
        peptides, _PRECURSOR_QVALUE, "pep"
    )
    if precursor_qval_col is not None:
        output_cols["Global.Precursor.Q.Value"] = _fns.col(precursor_qval_col)
        input_cols.add(precursor_qval_col)

    # Global Precursor PEP
    # TODO: semantics do not clarify global vs local; assume global for now
    precursor_errprob_col = _get_column_by_semantic(
        peptides, _PRECURSOR_ERRPROB, "pep"
    )
    if precursor_errprob_col is not None:
        output_cols["Global.Precursor.PEP"] = _fns.col(precursor_errprob_col)
        input_cols.add(precursor_errprob_col)

    # Global.Peptide.Q.Value: from peptide-level q-value semantic, if available
    # TODO: semantics do not clarify global vs local; assume global for now
    peptide_qval_col = _get_column_by_semantic(
        peptides, _PEPTIDE_QVALUE, "pep"
    )
    if peptide_qval_col is not None:
        output_cols["Global.Peptide.Q.Value"] = _fns.col(peptide_qval_col)
        input_cols.add(peptide_qval_col)

    # Global Peptide PEP
    # TODO: semantics do not clarify global vs local; assume global for now
    peptide_errprob_col = _get_column_by_semantic(
        peptides, _PEPTIDE_ERRPROB, "pep"
    )
    if peptide_errprob_col is not None:
        output_cols["Global.Peptide.PEP"] = _fns.col(peptide_errprob_col)
        input_cols.add(peptide_errprob_col)

    # Check that we have at least one q-value column
    if (
        psm_qval_col is None
        and precursor_qval_col is None
        and peptide_qval_col is None
    ):
        raise ValueError(
            "peptides dataset must have a PSM-, precursor-, or peptide-level q-value column for combined output"
        )

    # Global.PG.Q.Value: from protein group-level q-value semantic, or fall back to protein qvalue_column
    # TODO: semantics do not clarify global vs local; assume global for now
    pg_qval_col = _get_column_by_semantic(
        proteins, _PROTEIN_GROUP_QVALUE, "prot"
    )
    if pg_qval_col is None:
        pg_qval_col = getattr(proteins, "qvalue_column", None)
        if pg_qval_col is not None:
            pg_qval_col = f"prot.{pg_qval_col}"
    if pg_qval_col is not None:
        output_cols["Global.PG.Q.Value"] = _fns.col(pg_qval_col)
        input_cols.add(pg_qval_col)
    else:
        raise ValueError(
            "proteins dataset must have a q-value column for combined output"
        )

    # PG PEP: from protein group-level errprob sematnic, or fall back to protein errprob_column, if available
    # TODO: semantics do not clarify global vs local; assume global for now
    pg_errprob_col = _get_column_by_semantic(
        proteins, _PROTEIN_GROUP_ERRPROB, "prot"
    )
    if pg_errprob_col is None:
        pg_errprob_col = getattr(proteins, "errprob_column", None)
        if pg_errprob_col is not None:
            pg_errprob_col = f"prot.{pg_errprob_col}"
    if pg_errprob_col is not None:
        output_cols["Global.PG.PEP"] = _fns.col(pg_errprob_col)
        input_cols.add(pg_errprob_col)

    # Logic for quantity is somewhat complex, as there are three possible sources of truth:
    # - `PrecursorIntensityDataset.intensity_column`
    # - `XIC_AREA` semantic
    # - `NORMALIZED_XIC_AREA` semantic
    # We would like to prioritize the semantics if available, to permit reporting both raw and normalized intensities.
    # However, we should make an effort to always report as much intensity information as possible.

    intensity_col = _get_column_by_semantic(peptides, XIC_AREA, "pep")
    if intensity_col is not None:
        output_cols["Precursor.Quantity"] = _fns.col(intensity_col)
        input_cols.add(intensity_col)
    norm_intensity_col = _get_column_by_semantic(
        peptides, NORMALIZED_XIC_AREA, "pep"
    )
    if norm_intensity_col is not None:
        output_cols["Precursor.Normalised"] = _fns.col(norm_intensity_col)
        input_cols.add(norm_intensity_col)

    dset_intensity_col = getattr(peptides, "intensity_column", None)
    if dset_intensity_col is not None:
        dset_intensity_col = f"pep.{dset_intensity_col}"
        if dset_intensity_col not in input_cols:
            # The dataset's intensity_column` was not already included via semantics -- decide what to do with it.

            dset_intensity_semantic = peptides.semantics.get(
                dset_intensity_col, None
            )

            if intensity_col is None and norm_intensity_col is None:
                # No semantic intensity columns were found; report this one as `Precursor.Quantity`
                output_cols["Precursor.Quantity"] = _fns.col(
                    dset_intensity_col
                )
                input_cols.add(dset_intensity_col)

                if dset_intensity_semantic is None:
                    _logger.info(
                        "Using PSM intensity_column '%s' with undefined semantics for Precursor.Quantity",
                        dset_intensity_col,
                    )
                else:
                    _logger.info(
                        "Using PSM intensity_column '%s' with semantic '%s' for Precursor.Quantity",
                        dset_intensity_col,
                        dset_intensity_semantic,
                    )
            else:
                # Simply log a warning that this column is being dropped to avoid confusion
                if dset_intensity_semantic is None:
                    _logger.warning(
                        "PSM dataset's intensity_column '%s' with undefined semantics will be ignored",
                        dset_intensity_col,
                    )
                else:
                    _logger.warning(
                        "PSM dataset's intensity_column '%s' with semantic '%s' will be ignored",
                        dset_intensity_col,
                        dset_intensity_semantic,
                    )

    # PG.Quantity: from protein dataset intensity column if available
    # TODO: use semantics to resolve multiple roll-up quantities
    prot_intensity_col = getattr(proteins, "intensity_column", "prot")
    if prot_intensity_col is not None:
        output_cols["PG.Quantity"] = _fns.col(prot_intensity_col)
        input_cols.add(prot_intensity_col)
    else:
        raise ValueError(
            "proteins dataset must have an intensity column for combined output"
        )

    # Add score columns if not already included
    for score_col in peptides.score_columns:
        score_input_col = f"pep.{score_col}"
        if score_input_col not in input_cols:
            if score_col not in output_cols.keys():
                output_cols[score_col] = _fns.col(score_input_col)
            else:
                # Avoid column name collision
                _logger.warning(
                    "score column %s already exists in output; it will be dropped!",
                    score_input_col,
                )
    for score_col in proteins.score_columns:
        score_input_col = f"prot.{score_col}"
        if score_input_col not in input_cols:
            if score_col not in output_cols.keys():
                output_cols[f"PG.{score_col}"] = _fns.col(score_input_col)
            else:
                # Avoid column name collision
                _logger.warning(
                    "score column %s already exists in output; it will be dropped!",
                    score_input_col,
                )

    result_df = joined.select(
        *[c.alias(name) for name, c in output_cols.items()]
    )

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Will write combined results for %d rows…", result_df.count()
        )

    _kwargs = kwargs.copy()

    result_df.write.parquet(
        path=location,
        mode=_kwargs.pop("mode", "errorifexists"),
        **_kwargs,
    )
