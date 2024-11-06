"""
`scry.workflows.v1`: Basic ID and quantification workflow packaged in Scry v1
"""

import logging as _logging
from time import time as _time
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
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
from proffer.spark import infer_spark as _infer_spark
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
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
    search: _Dict[str, _Any] = None,
    airpot: _Dict[str, _Any] = None,
    cortado: _Dict[str, _Any] = None,
    peptide_quant=None,  # TODO
    proffer=None,  # TODO
    protein_scoring=None,  # TODO
    protein_rollup=None,  # TODO
    output: _Union[str, _Dict[str, _Any]] = None,
    # TODO: protein output?
    spark: _SparkSession = None,
) -> _ConfidenceDataset:
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
        TODO

    proffer: dict, optional
        TODO

    protein_scoring: dict, optional
        TODO

    protein_rollup: dict, optional
        TODO

    output: str|dict, optional
        TODO: revise output support!
        Any arguments to use for outputting FDR-controlled results. If a string, it will be
        passed to the `location` keyword of the default backend. If unspecified, None, or empty
        no output will be written.

        Special keys:
        - `backend`: The backend that will compute or read search results. Default: `write_parquet`
           Either a string referring to a backend or plugin, or a callable. If a callable the
           `ConfidenceDataset` will be passed as the first argument, and any other items in the
           dict will be passed as keyword arguments.

    # TODO: protein output?

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
    _logger.info(
        "Found %d PSMs or peptides at %.0f%% FDR",
        conf.data.filter(conf.qvalues <= test_fdr).count(),
        100 * test_fdr,
    )

    peptide_quant = peptide_quant.copy() if peptide_quant else dict()
    quant_backend = peptide_quant.pop("backend", "basic")
    if not callable(quant_backend):
        quant_backend = _get_peptide_quant_backend(quant_backend)

    peptide_quant = quant_backend(
        conf,
        **peptide_quant,
    )

    # Get a table of peptides with their group and proteotypicity
    inference = _infer_spark(conf, **(proffer or dict()))

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

    res_inferred = conf.with_data(
        conf.data.join(
            inference_spark,
            on=conf.peptide_column,
            how="leftouter",
        ),
        protein_column="protein_group",
        protein_delim=protein_delim,
    )

    # Compute protein FDR
    from cortado.protein import score_proteins

    protein_result = score_proteins(
        res_inferred,
        **(protein_scoring or {}),
    )

    protein_rollup = protein_rollup.copy() if protein_rollup else dict()
    rollup_backend = protein_rollup.pop("backend", "basic")
    if not callable(rollup_backend):
        rollup_backend = _get_protein_rollup_backend(rollup_backend)

    protein_quant = rollup_backend(
        peptide_quant,
        **protein_rollup,
    )

    protein_result = protein_result.with_data(
        protein_result.data.join(
            protein_quant.data.select(
                (
                    # To match the handling of protein group IDs above, we must join group IDs into strings
                    _fns.array_join(
                        (
                            protein_quant.proteins
                            if protein_quant.protein_delim is None
                            else _fns.split(
                                protein_quant.proteins,
                                protein_quant.protein_delim,
                            )
                        ),
                        protein_result.protein_delim,
                    )
                ).alias(protein_result.protein_column),
                protein_quant.samples,
                protein_quant.intensities,
            ),
            on=protein_result.protein_column,
            how="left",
        )
    )

    if output:
        # TODO: revise output support!
        assert output == dict(
            backend=None
        ), "TODO: only output.backend=None is supported!"

        if isinstance(output, str):
            output = dict(location=output)

        output_backend = output.pop("backend", "write_parquet")
        if not callable(output_backend):
            output_backend = _get_output_backend(output_backend)

        output_start = _time()

        output_result = output_backend(conf, **output)

        output_end = _time()

        _logger.info(
            "Wrote results for %d PSMs or peptides in %.02f sec",
            n_confs,
            output_end - output_start,
        )
    else:
        output_result = None

    # TODO: protein output?

    # TODO: protein result?
    return conf if output_result is None else output_result
