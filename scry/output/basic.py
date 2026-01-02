"""
`scry.output.confidence`: output implementations for confidence results
"""

import logging as _logging
from time import time as _time
from typing import (
    Callable as _Callable,
    Optional as _Optional,
    Union as _Union,
)

from pyspark.sql import (
    Column as _Column,
)

from wheely.mammoth import PsmDataset as _PsmDataset
from wheely.mammoth.proteins import ProteinDataset as _ProteinDataset

from .util import filter_psms

_logger = _logging.getLogger(__name__)


def write_csv(
    peptides: _PsmDataset,
    proteins: _Optional[_ProteinDataset],
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    peptide_kwargs: _Optional[dict] = None,
    protein_kwargs: _Optional[dict] = None,
    **kwargs,
):
    """
    Write the given dataset(s) to CSV.

    Primarily calls ``pyspark.sql.DataFrameWrite.csv`` separately for each dataset.

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
        (Optional) The protein dataset to write
    location : str
        A string specifying where the results should be written. By default the peptide and protein datasets will be
        written to ``{location}/scry-peptides`` and ``{location}/scry-proteins`` respectively.
    threshold_col : str | pyspark.sql.Column
        (Optional) A column (or its name) specifying which rows will be included in the resulting library.
    qval_thresh : float
        (Optional) The largest *q*-value accepted into the library. Ignored if the dataset is not a
        ``wheely.mammoth.ConfidenceDataset`` or ``threshold_col`` is specified.
    include_decoys : bool
        (Optional) If true, include decoy PSMs in the library. Ignored if ``threshold_col`` is specified. Default: ``False``
    peptide_kwargs : dict (optional)
        (Optional) Overrides used only for the peptide dataset.
    protein_kwargs : dict (optional)
        (Optional) Overrides used only for the protein dataset.
    kwargs :
        additional keyword arguments to pass to :py:func:`pyspark.sql.DataFrameWrite.csv`.
        Defaults: ``{"mode": "errorifexists", "header": True}``
    """
    _write_basic(
        _write_csv,
        peptides=peptides,
        proteins=proteins,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
        peptide_kwargs=peptide_kwargs,
        protein_kwargs=protein_kwargs,
        **kwargs,
    )


def _write_csv(
    data: _Union[_PsmDataset, _ProteinDataset],
    location: str,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    **kwargs,
):
    """
    Write the given dataset to CSV.

    A thin wrapper around ``pyspark.sql.DataFrameWrite.csv`` separately for each dataset.

    **Filtering** -- The ``qval_thresh`` and ``include_decoys`` parameters allow convenient filtering of output
    PSMs or proteins.

    For more sophisticated filtering, the optional ``threshold_col`` parameter includes only rows where this column
    is ``True`` in the output. When ``threshold_col`` is specified the ``qval_thresh`` and ``include_decoys`` parameters
    will be ignored.

    Parameters
    ----------
    data : ConfidenceDataset
        The dataset to write
    location : str
        A string specifying where the results should be written
    threshold_col : str | pyspark.sql.Column
        (Optional) A column (or its name) specifying which rows will be included in the resulting library.
    qval_thresh : float
        (Optional) The largest *q*-value accepted into the library. Ignored if the dataset is not a
        ``wheely.mammoth.ConfidenceDataset`` or ``threshold_col`` is specified.
    include_decoys : bool
        (Optional) If true, include decoy PSMs in the library. Ignored if ``threshold_col`` is specified. Default: ``False``
    kwargs :
        additional keyword arguments to pass to :py:func:`pyspark.sql.DataFrameWrite.csv`.
        Defaults: ``{"mode": "errorifexists", "header": True}``
    """

    data_filt = filter_psms(
        data,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
    )

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info("Will write results for %d rows…", data_filt.data.count())

    _kwargs = kwargs.copy()

    data_filt.data.write.csv(
        path=location,
        mode=_kwargs.pop("mode", "errorifexists"),
        header=_kwargs.pop("header", True),
        **_kwargs,
    )


def write_parquet(
    peptides: _PsmDataset,
    proteins: _Optional[_ProteinDataset],
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    peptide_kwargs: _Optional[dict] = None,
    protein_kwargs: _Optional[dict] = None,
    **kwargs,
):
    """
    Write the given dataset(s) to Parquet.

     Primarily calls ``pyspark.sql.DataFrameWrite.parquet``.

    **Filtering** -- The``qval_thresh`` and ``include_decoys`` parameters allow convenient filtering of output
    PSMs or proteins.

    For more sophisticated filtering, the optional ``threshold_col`` parameter includes only rows where this column
    is ``True`` in the output. When ``threshold_col`` is specified the ``qval_thresh`` and ``include_decoys`` parameters
    will be ignored.

    Parameters
    ----------
    peptides : PsmDataset
        The peptide dataset to write
    proteins : ProteinDataset
        (Optional) The protein dataset to write
    location : str
        A string specifying where the results should be written. By default the peptide and protein datasets will be
        written to ``{location}/scry-peptides`` and ``{location}/scry-proteins`` respectively.
    threshold_col : str | pyspark.sql.Column
        (Optional) A column (or its name) specifying which rows will be included in the resulting library.
    qval_thresh : float
        (Optional) The largest *q*-value accepted into the library. Ignored if the dataset is not a
        ``wheely.mammoth.ConfidenceDataset`` or ``threshold_col`` is specified.
    include_decoys : bool
        (Optional) If true, include decoy PSMs in the library. Ignored if ``threshold_col`` is specified. Default: ``False``
    peptide_kwargs : dict (optional)
        (Optional) Overrides used only for the peptide dataset.
    protein_kwargs : dict (optional)
        (Optional) Overrides used only for the protein dataset.
    kwargs :
        additional keyword arguments to pass to :py:func:`pyspark.sql.DataFrameWrite.csv`.
        Defaults: ``{"mode": "errorifexists", "header": True}``
    """
    _write_basic(
        _write_parquet,
        peptides=peptides,
        proteins=proteins,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
        peptide_kwargs=peptide_kwargs,
        protein_kwargs=protein_kwargs,
        **kwargs,
    )


def _write_parquet(
    data: _Union[_PsmDataset, _ProteinDataset],
    location: str,
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    **kwargs,
):
    """
    Write the given dataset to Parquet. A thin wrapper around ``pyspark.sql.DataFrameWrite.parquet``.

    **Filtering** -- The``qval_thresh`` and ``include_decoys`` parameters allow convenient filtering of output
    PSMs or proteins.

    For more sophisticated filtering, the optional ``threshold_col`` parameter includes only rows where this column
    is ``True`` in the output. When ``threshold_col`` is specified the ``qval_thresh`` and ``include_decoys`` parameters
    will be ignored.

    Parameters
    ----------
    data : ConfidenceDataset
        The dataset to write
    location : str
        The location where the results should be written. Typically a file path or URI understood by Spark.
    threshold_col : str | pyspark.sql.Column
        (Optional) A column (or its name) specifying which rows will be included in the resulting library.
    qval_thresh : float
        (Optional) The largest *q*-value accepted into the library. Ignored if the dataset is not a
        ``wheely.mammoth.ConfidenceDataset`` or ``threshold_col`` is specified.
    include_decoys : bool
        (Optional) If true, include decoy PSMs in the library. Ignored if ``threshold_col`` is specified. Default: False
    kwargs : additional keyword arguments to pass to ``pyspark.sql.DataFrameWrite.parquet``.
              Defaults: ``{"mode": "errorifexists"}``
    """

    data_filt = filter_psms(
        data,
        threshold_col=threshold_col,
        qval_thresh=qval_thresh,
        include_decoys=include_decoys,
    )

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info("Will write results for %d rows…", data_filt.data.count())

    _kwargs = kwargs.copy()

    data_filt.data.write.parquet(
        path=location,
        mode=_kwargs.pop("mode", "errorifexists"),
        **_kwargs,
    )


def _write_basic(
    fn: _Callable,
    peptides: _PsmDataset,
    proteins: _Optional[_ProteinDataset],
    threshold_col: _Optional[_Union[str, _Column]] = None,
    qval_thresh: float = None,
    include_decoys: bool = False,
    peptide_kwargs: _Optional[dict] = None,
    protein_kwargs: _Optional[dict] = None,
    **kwargs,
):
    """
    Write the given dataset(s) to CSV.

    Primarily calls ``pyspark.sql.DataFrameWrite.csv`` separately for each dataset.

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
        (Optional) The protein dataset to write
    location : str
        A string specifying where the results should be written. By default the peptide and protein datasets will be
        written to ``{location}/scry-peptides`` and ``{location}/scry-proteins`` respectively.
    threshold_col : str | pyspark.sql.Column
        (Optional) A column (or its name) specifying which rows will be included in the resulting library.
    qval_thresh : float
        (Optional) The largest *q*-value accepted into the library. Ignored if the dataset is not a
        ``wheely.mammoth.ConfidenceDataset`` or ``threshold_col`` is specified.
    include_decoys : bool
        (Optional) If true, include decoy PSMs in the library. Ignored if ``threshold_col`` is specified. Default: ``False``
    peptide_kwargs : dict (optional)
        (Optional) Overrides used only for the peptide dataset.
    protein_kwargs : dict (optional)
        (Optional) Overrides used only for the protein dataset.
    kwargs :
        additional keyword arguments to pass to :py:func:`pyspark.sql.DataFrameWrite.csv`.
        Defaults: ``{"mode": "errorifexists", "header": True}``
    """

    peptide_kwargs = dict(
        kwargs,
        **peptide_kwargs,
    )

    pep_out_start = _time()
    pep_out_res = fn(peptides, **peptide_kwargs)
    pep_out_end = _time()

    _logger.info(
        "Wrote peptide results in %.02f sec",
        pep_out_end - pep_out_start,
    )

    pep_res = pep_out_res or peptides

    protein_kwargs = dict(
        kwargs,
        **protein_kwargs,
    )

    prot_out_start = _time()
    prot_out_res = fn(proteins, **protein_kwargs)
    prot_out_end = _time()

    _logger.info(
        "Wrote protein results in %.02f sec",
        prot_out_end - prot_out_start,
    )

    prot_res = prot_out_res or proteins

    return pep_res, prot_res
