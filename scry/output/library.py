"""
`scry.output.library` -- workflow module that supports transforming sets of PSMs into a format
suitable for use with library search tools.
"""
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)


def write_library(data: _PsmDataset, location: str, **kwargs):
    """
    Write the given dataset to the given location.

    This implementation is still in progress:
    * TODO: define the format in which the library will be written
    * TODO: define the source of spectral / RT information
    * TODO: define any filtering applied to the output library

    Future directions:
    * TODO: add support for other dimensions: e.g. IM

    Returns
    -------
    TODO: document return
    """
    raise NotImplementedError("TODO")
