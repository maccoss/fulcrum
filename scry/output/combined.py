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
from wheely.mammoth.semantics.semantics import (
    PSM_QVALUE as _PSM_QVALUE,
    PRECURSOR_QVALUE as _PRECURSOR_QVALUE,
    PSM_PRECURSOR_QVALUE as _PSM_PRECURSOR_QVALUE,
    PROTEIN_GROUP_QVALUE as _PROTEIN_GROUP_QVALUE,
    RT_IN_SECONDS as _RT_IN_SECONDS,
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

    # Modified.Sequence: from peptide_column
    output_cols["Modified.Sequence"] = _fns.col(
        f"pep.{peptides.peptide_column}"
    )
    input_cols.add(f"pep.{peptides.peptide_column}")

    # Precursor.Charge: from charge_column, if available
    if peptides.charge_column is not None:
        output_cols["Precursor.Charge"] = _fns.col(
            f"pep.{peptides.charge_column}"
        )
        input_cols.add(f"pep.{peptides.charge_column}")

    # RT: from RT semantic, or fall back to rt_column if available
    rt_col = _get_column_by_semantic(peptides, _RT_IN_SECONDS, "pep")
    if rt_col is None:
        rt_col = getattr(peptides, "rt_column", None)
        if rt_col is not None:
            rt_col = f"pep.{rt_col}"
    if rt_col is not None:
        output_cols["RT"] = _fns.col(rt_col)
        input_cols.add(rt_col)

    # Q.Value: from PSM-level q-value semantic, if available
    psm_qval_col = _get_column_by_semantic(peptides, _PSM_QVALUE, "pep")
    if psm_qval_col is not None:
        output_cols["Q.Value"] = _fns.col(psm_qval_col)
        input_cols.add(psm_qval_col)

    # PEP: from errprob_column, if available
    errprob_col = getattr(peptides, "errprob_column", "pep")
    if errprob_col is not None:
        output_cols["PEP"] = _fns.col(errprob_col)
        input_cols.add(errprob_col)

    # Global.Precursor.Q.Value: from precursor-level q-value semantic, if available
    precursor_qval_col = _get_column_by_semantic(
        peptides, _PRECURSOR_QVALUE, "pep"
    )
    if precursor_qval_col is not None:
        output_cols["Global.Precursor.Q.Value"] = _fns.col(precursor_qval_col)
        input_cols.add(precursor_qval_col)

    if psm_qval_col is None and precursor_qval_col is None:
        raise ValueError(
            "peptides dataset must have a PSM- or precursor q-value column for combined output"
        )

    # Global.PG.Q.Value: from protein group-level q-value semantic, or fall back to protein qvalue_column
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

    # PG PEP: from protein errprob_column, if available
    pg_errprob_col = getattr(proteins, "errprob_column", "prot")
    if pg_errprob_col is not None:
        output_cols["PG.PEP"] = _fns.col(pg_errprob_col)
        input_cols.add(pg_errprob_col)

    # Precursor.Quantity: from precursor dataset intensity column if available
    # TODO: use semantics to resolve raw vs normalized intensity
    pep_intensity_col = getattr(peptides, "intensity_column", "pep")
    if pep_intensity_col is not None:
        output_cols["Precursor.Quantity"] = _fns.col(pep_intensity_col)
        input_cols.add(pep_intensity_col)
    else:
        raise ValueError(
            "peptides dataset must have an intensity column for combined output"
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
                output_cols[score_col] = _fns.col(score_input_col)
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
