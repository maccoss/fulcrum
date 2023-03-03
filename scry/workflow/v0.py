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

from airpot import (
    brew as _brew,
    BrewResult as _BrewResult,
    RescoringResult as _RescoringResult,
)
from wheely.mammoth import PsmDataset as _PsmDataset

from ..search import (
    search_backends as _search_backends,
)

_logger = _logging.getLogger(__name__)


def scry_v0(
    search_backend: _Union[str, _Callable[..., _PsmDataset]] = "read_existing",
    search_kwargs: _Dict[str, _Any] = dict(),
    airpot_kwargs: _Dict[str, _Any] = dict(),
):
    """
    scry_v0: initial experimental workflow

    Parameters
    ----------
    search_backend: SearchBackend, optional
        The backend that will compute or read search results. Default: `read_existing`
    search_kwargs: dict
        Any arguments to pass to the search backend. Notably this will typically include the
        location(s) of any input samples / files necessary to execute the search.
    airpot_kwargs: dict, optional
        Any arguments to pass to `airpot` for rescoring the dataset.
    """
    if not isinstance(search_backend, _Callable):
        search_backend = _search_backends[search_backend]

    search_start = _time()

    psms: _PsmDataset = search_backend(**search_kwargs)

    search_end = _time()

    if not psms:
        raise ValueError("Got invalid PSM dataset from search backend!")

    _logger.info(
        "Search stage found %d PSMs in %.02f sec",
        psms.data.count(),
        search_end - search_start,
    )

    model_start = _time()

    model: _BrewResult = _brew(psms, **airpot_kwargs)

    model_end = _time()

    _logger.info("Built rescoring model in %.02f sec", model_end - model_start)

    rescored: _PsmDataset
    if "subset_size" in airpot_kwargs and psms.data.count() > int(
        airpot_kwargs.pop("subset_size")
    ):
        # The user requested that airpot train on only a subset of the PSMs
        # so we will need to rescore them.

        score_start = _time()

        rescoring_result: _RescoringResult = _brew(
            psms,
            model=model,
            **airpot_kwargs,  # we've popped subset_size out of this dict
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

    # TODO: remaining parts of pipeline
