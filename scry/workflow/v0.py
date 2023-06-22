"""
`scry.workflows.v0`: Initial workflow packaged in Scry v0
"""

import logging as _logging
from time import time as _time
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
    Union as _Union,
)

from pyspark.sql import SparkSession as _SparkSession

from airpot import (
    brew as _brew,
    BrewResult as _BrewResult,
    RescoringResult as _RescoringResult,
)
from cortado import assign_confidence as _assign_confidence
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)

from ..search import (
    get_backend as _get_search_backend,
)
from ..output import (
    get_backend as _get_output_backend,
)

_logger = _logging.getLogger(__name__)


def scry_v0(
    search: _Dict[str, _Any] = dict(),
    airpot: _Dict[str, _Any] = dict(),
    cortado: _Dict[str, _Any] = dict(),
    output: _Union[str, _Dict[str, _Any]] = None,
    spark: _SparkSession = None,
) -> _ConfidenceDataset:
    """
    scry_v0: initial experimental workflow

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

    output: str|dict, optional
        Any arguments to use for outputting FDR-controlled results. If a string, it will be
        passed to the `location` keyword of the default backend. If unspecified, None, or empty
        no output will be written.

        Special keys:
        - `backend`: The backend that will compute or read search results. Default: `write_csv`
           Either a string referring to a backend or plugin, or a callable. If a callable the
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

    model: _BrewResult = _brew(psms, **airpot)

    model_end = _time()

    _logger.info("Built rescoring model in %.02f sec", model_end - model_start)

    rescored: _PsmDataset
    if "subset_size" in airpot and psms.data.count() > int(
        airpot.pop("subset_size")
    ):
        # The user requested that airpot train on only a subset of the PSMs
        # so we will need to rescore them.

        score_start = _time()

        rescoring_result: _RescoringResult = _brew(
            psms,
            model=model,
            **airpot,  # we've popped subset_size out of this dict
        )

        score_end = _time()

        rescored = rescoring_result.psms

        _logger.info(
            "Rescored %d PSMs in %.02f sec",
            rescored.data.count(),
            score_end - score_start,
        )
    else:
        try:
            rescored = model.psms
        except (AttributeError, KeyError) as e:
            raise TypeError(
                "Expected a RescoringResult, got " + type(model)
            ) from e

    assert (
        rescored is not None and rescored.data.count() > 0
    ), "Did not get any rescored PSMs!"

    # assume the first score is the rescored one
    score_name = next(iter(rescored.score_columns))

    _logger.info(
        'Assigning confidence across the dataset using "%s" (ascending)',
        score_name,
    )

    conf_start = _time()

    conf = _assign_confidence(
        rescored,
        score_column=score_name,
        desc=False,  # just assume rescoring gives an increasing score
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

    if output:
        if isinstance(output, str):
            output = dict(location=output)

        output_backend = output.pop("backend", "write_csv")
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

    return conf if output_result is None else output_result
