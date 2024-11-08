"""
`scry.workflows.v1`: Basic ID and quantification workflow packaged in Scry v1
"""

import logging as _logging
from time import time as _time
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
    Optional as _Optional,
    Tuple as _Tuple,
    Union as _Union,
)

import polars as _pl
from pyspark.sql import (
    SparkSession as _SparkSession,
    functions as _fns,
)

from airpot import (
    brew as _brew,
    BrewResult as _BrewResult,
    RescoringResult as _RescoringResult,
)
from cortado import assign_confidence as _assign_confidence
from cortado.protein import score_proteins as _score_proteins
from proffer.spark import infer_spark as _infer_spark
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)
from wheely.mammoth.proteins import (
    ProteinDataset as _ProteinDataset,
)

from ..search import (
    get_backend as _get_search_backend,
)
from ..quant.peptide import get_backend as _get_peptide_quant_backend
from ..quant.protein import get_backend as _get_protein_rollup_backend
from ..output import (
    get_backend as _get_output_backend,
)

_logger = _logging.getLogger(__name__)


def scry_v1(
    search: _Optional[_Dict[str, _Any]] = None,
    airpot: _Optional[_Dict[str, _Any]] = None,
    cortado: _Optional[_Dict[str, _Any]] = None,
    peptide_quant: _Optional[_Dict[str, _Any]] = None,
    proffer: _Optional[_Dict[str, _Any]] = None,
    protein_scoring: _Optional[_Dict[str, _Any]] = None,
    protein_rollup: _Optional[_Dict[str, _Any]] = None,
    output: _Optional[_Dict[str, _Any]] = None,
    peptide_output: _Optional[_Union[str, _Dict[str, _Any]]] = None,
    protein_output: _Optional[_Union[str, _Dict[str, _Any]]] = None,
    spark: _Optional[_SparkSession] = None,
) -> _Tuple[_PsmDataset, _ProteinDataset]:
    """
    scry_v1: basic ID/quant workflow

    Parameters
    ----------
    search: dict, optional
        Any arguments to pass to the search backend. Notably this will typically include the
        location(s) of any input samples / files necessary to execute the search.

        Special keys:
        - `backend`: The backend that will compute or read search results. Default: `read_existing`

    airpot: dict, optional
        Any arguments to pass to `airpot` for rescoring search results.

    cortado: dict, optional
        Any arguments to pass to `cortado` for confidence estimation on the rescored dataset.

    peptide_quant: dict, optional,
        Any arguments to use for computing PSM quantities.

        Special keys:
        - `backend`: The backend to run. Either the name of the backend, or a callable. Default: `basic`

    proffer: dict, optional
        Any arguments to pass to `proffer.infer_spark`. In typical usage, it's strongly recommended to specify
        a `qvalue_threshold`!

    protein_scoring: dict, optional
        Any arguments to `cortado.protein.score_proteins` for protein scoring and (optinal) confidence estimation.

    protein_rollup: dict, optional
        Any arguments to use for computing protein quantities.

        Special keys:
        - `backend`: The backend to run. Either the name of the backend, or a callable. Default: `basic`

    output: dict, optional
        Default parameters for outputting both peptide and protein level results. Parameters given here will
        be overridden by the `peptide_output` or `protein_output` parameter if one or both is specified.

        Special keys:
        - `backend`: The backend that will write results. Default: `write_parquet`
           Either a string referring to a backend or plugin, or a `Callable`. If a callable the
           `ConfidenceDataset` will be passed as the first argument, and any other items in the
           dict will be passed as keyword arguments.

    peptide_output: str | dict, optional
        If a string, the location for peptide output, in which case the backend and parameters from `output` will
        be used. If a dict, parameters for output peptide-level results.

        Special keys:
        - `backend`: The backend that will write peptide-level results. Default: `write_parquet`
           Either a string referring to a backend or plugin, or a `Callable`. If a callable the
           `ConfidenceDataset` will be passed as the first argument, and any other items in the
           dict will be passed as keyword arguments.

    protein_output: str | dict, optional
        If a string, the location for protein output, in which case the backend and parameters from `output` will
        be used. If a dict, parameters for output protein-level results.

        Special keys:
        - `backend`: The backend that will write protein-level results. Default: `write_parquet`
           Either a string referring to a backend or plugin, or a `Callable`. If a callable the
           `ConfidenceDataset` will be passed as the first argument, and any other items in the
           dict will be passed as keyword arguments.

    spark: SparkSession, optional
        A Spark session to use when creating the search result dataset. If `None` a session
        will be created with default configuration.

    Returns
    -------
    Either the results of the `output` module (if `output` is truthy and the backend returns a
    value other than `None`). If no `output` is specified, or if the output backend returns `None`
    this workflow will return the results from the `cortado` module.
    """
    search = search or dict()
    search_backend = search.pop("backend", "read_existing")
    if not callable(search_backend):
        search_backend = _get_search_backend(search_backend)

    search_start = _time()

    psms: _PsmDataset = search_backend(**search, spark=spark)

    search_end = _time()

    if not psms:
        raise ValueError("Got invalid PSM dataset from search backend!")

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Search stage found %d PSMs in %.02f sec",
            psms.data.count(),
            search_end - search_start,
        )

    model_start = _time()

    model: _BrewResult = _brew(psms, **(airpot or dict()))

    model_end = _time()

    _logger.info("Built rescoring model in %.02f sec", model_end - model_start)

    rescored: _PsmDataset
    try:
        rescored = model.psms
    except (AttributeError, KeyError) as e:
        raise TypeError(
            "Expected a RescoringResult, got " + type(model)
        ) from e

    assert (
        rescored is not None and rescored.data.count() > 0
    ), "Did not get any rescored PSMs!"

    # Allow mutation
    cortado = cortado.copy() if cortado else dict()

    # If unspecified, assume the first score is the rescored one
    score_name = cortado.pop(
        "score_column", next(iter(rescored.score_columns))
    )
    desc = cortado.pop("desc", False)

    _logger.info(
        'Assigning confidence across the dataset using "%s" (%sscending)',
        score_name,
        "a" if not desc else "de",
    )

    conf_start = _time()

    conf = _assign_confidence(
        rescored,
        score_column=score_name,
        desc=desc,
        **cortado,
    )

    conf_end = _time()

    n_confs = conf.data.count()
    _logger.info(
        "Assigned confidence to %d PSMs or peptides in %.02f sec",
        n_confs,
        conf_end - conf_start,
    )

    test_fdr = 0.01  # TODO
    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Found %d PSMs or peptides at %.0f%% FDR",
            conf.data.filter(conf.qvalues <= test_fdr).count(),
            100 * test_fdr,
        )

    # Get a table of peptides with their group and proteotypicity
    inf_start = _time()
    inference = _infer_spark(conf, **(proffer or dict()))
    inf_end = _time()

    # The result from Proffer will have list-valued protein groups; we need to join back together IDs
    # into a single string to get them into Spark via Pandas; ideally this could be avoided by using
    # Arrow instead as the interchange between Polars and Spark.
    protein_delim = conf.protein_delim or ";"
    inference_spark = conf.data.sparkSession.createDataFrame(
        inference.select(
            _pl.col("peptide").alias(conf.peptide_column),
            _pl.col("protein_group")
            .list.unique()
            .list.sort()
            .list.join(protein_delim),
            "proteotypic",
        ).to_pandas()
    )

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Inferred %d protein groups in %.02f sec",
            inference_spark.select(_fns.countDistinct("protein_group"))
            .toPandas()
            .iloc[0, 0],
            inf_end - inf_start,
        )

    res_inferred = conf.with_data(
        conf.data.join(
            inference_spark,
            on=conf.peptide_column,
            how="leftouter",
        ),
        protein_column="protein_group",
        protein_delim=protein_delim,
    )

    # Compute protein FDR by rescoring protein groups
    # TODO: pluggable backends
    prot_conf_start = _time()
    prot_conf = _score_proteins(
        res_inferred,
        **(protein_scoring or {}),
    )
    prot_conf_end = _time()

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Scored %d protein groups in %.02f sec",
            prot_conf.data.count(),
            prot_conf_end - prot_conf_start,
        )

    if (
        c := getattr(prot_conf, "qvalue_column", None)
    ) is not None and _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Found %d protein groups at %.0f%% FDR",
            prot_conf.data.filter(_fns.col(c) <= test_fdr).count(),
            100 * test_fdr,
        )

    # Quantify peptides after they're annotated with groups, to simplify rollup
    peptide_quant = peptide_quant.copy() if peptide_quant else dict()
    quant_backend = peptide_quant.pop("backend", "basic")
    if not callable(quant_backend):
        quant_backend = _get_peptide_quant_backend(quant_backend)

    quant_start = _time()
    pep_quant_dset = quant_backend(
        res_inferred,
        **peptide_quant,
    )
    quant_end = _time()

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Quantified %d peptides in %.02f sec",
            pep_quant_dset.data.count(),
            quant_end - quant_start,
        )

    protein_rollup = protein_rollup.copy() if protein_rollup else dict()
    rollup_backend = protein_rollup.pop("backend", "basic")
    if not callable(rollup_backend):
        rollup_backend = _get_protein_rollup_backend(rollup_backend)

    rollup_start = _time()
    prot_quant_dset = rollup_backend(
        pep_quant_dset,
        **protein_rollup,
    )
    rollup_end = _time()

    _logger.info(
        "Rolled up protein group intensities in %.02f sec",
        rollup_end - rollup_start,
    )

    protein_result = prot_conf.with_data(
        prot_conf.data.join(
            prot_quant_dset.data.select(
                (
                    # To match the handling of protein group IDs above, we must join group IDs into strings
                    _fns.array_join(
                        (
                            prot_quant_dset.proteins
                            if prot_quant_dset.protein_delim is None
                            else _fns.split(
                                prot_quant_dset.proteins,
                                prot_quant_dset.protein_delim,
                            )
                        ),
                        prot_conf.protein_delim,
                    )
                ).alias(prot_conf.protein_column),
                prot_quant_dset.samples,
                prot_quant_dset.intensities,
            ),
            on=prot_conf.protein_column,
            how="leftouter",
        )
    )

    # Output peptide results
    if isinstance(peptide_output, str):
        pep_out_loc = peptide_output
        peptide_output = dict(
            output or dict(),
            **dict(
                location=pep_out_loc,
            ),
        )
    elif peptide_output:
        peptide_output = peptide_output.copy()
    else:
        peptide_output = dict()

    peptide_out_backend = peptide_output.pop("backend", "write_parquet")
    if not callable(peptide_out_backend):
        peptide_out_backend = _get_output_backend(peptide_out_backend)

    pep_out_start = _time()
    pep_out_res = peptide_out_backend(pep_quant_dset, **peptide_output)
    pep_out_end = _time()

    _logger.info(
        "Wrote peptide results in %.02f sec",
        pep_out_end - pep_out_start,
    )

    pep_res = pep_out_res or pep_quant_dset

    # Output protein results
    if isinstance(protein_output, str):
        prot_out_loc = protein_output
        protein_output = dict(
            output or dict(),
            **dict(
                location=prot_out_loc,
            ),
        )
    elif protein_output:
        protein_output = protein_output.copy()
    else:
        protein_output = dict()

    protein_out_backend = protein_output.pop("backend", "write_parquet")
    if not callable(protein_out_backend):
        protein_out_backend = _get_output_backend(protein_out_backend)

    prot_out_start = _time()
    prot_out_res = protein_out_backend(protein_result, **protein_output)
    prot_out_end = _time()

    _logger.info(
        "Wrote peptide results in %.02f sec",
        prot_out_end - prot_out_start,
    )

    prot_res = prot_out_res or protein_result

    return pep_res, prot_res
