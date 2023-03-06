"""
`scry.scry`: this module contains the main entry point for Scry workflows
"""
from pyspark.sql import SparkSession as _SparkSession

from .workflow.v0 import scry_v0

_workflows = {
    "v0": scry_v0,
}


def scry(
    workflow: str = "v0",
    spark: _SparkSession = None,
    **kwargs,
):
    """
    `scry()`: run a Scry workflow using the specified parameters

    Parameters
    ----------
    workflow: str, optional
        The name of a packaged workflow. Default: "v0"
    **kwargs
        Any keyword arguments are passed directly to the workflow
    """

    result = _workflows[workflow](spark=spark, **kwargs)

    if spark is None:
        # If the caller did not pass in a Spark session, assume one was created.
        # To be a good citizen, clean it up now that we're done with it.
        # NOTE: if we begin returning a dataset object we should likely remove this code and place
        # the responsibility on the caller.
        result.data.sparkSession.stop()
