"""
`scry.workflows.v0`: Initial workflow packaged in Scry v0
"""

import logging as _logging
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
    Union as _Union,
)

from wheely.mammoth import PsmDataset as _PsmDataset

from ..search import (
    search_backends as _search_backends,
)

_logger = _logging.getLogger(__name__)


def scry_v0(
    search_backend: _Union[str, _Callable[..., _PsmDataset]] = "read_existing",
    search_kwargs: _Dict[str, _Any] = dict(),
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
    """
    if not isinstance(search_backend, _Callable):
        search_backend = _search_backends[search_backend]

    psms: _PsmDataset = search_backend(**search_kwargs)

    if not psms:
        raise ValueError("Got invalid PSM dataset from search backend!")

    _logger.info("Search stage found %d PSMs", psms.data.count())
