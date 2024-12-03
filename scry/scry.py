"""
`scry.scry`: this module contains the main entry point for Scry workflows
"""

from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
    Optional as _Optional,
    Union as _Union,
)
import logging as _logging

from pyspark.sql import SparkSession as _SparkSession

from wheely.mammoth import (
    PsmDataset as _PsmDataset,
)

from .workflow import get_workflow as _get_workflow

_logger = _logging.getLogger(__name__)


def scry(
    workflow: _Union[str, _Callable[..., _PsmDataset]] = "v0",
    spark: _SparkSession = None,
    spark_config: _Optional[_Dict[str, _Any]] = None,
    **kwargs,
) -> _PsmDataset:
    """
    Run a Scry workflow using the specified parameters

    Parameters
    ----------
    workflow: str|Callable, optional
        The name of registered workflow, or a callable.

        For a list of built-in workflows, see :py:mod:`scry.workflow`.

        Default: `"v0"`
    spark: SparkSession, optional
        A Spark session object that will be passed to the workflow;
        this session will be used to execute computations in the workflow.

        If unspecified, a Spark session will be created using the config
        from `spark_config`.
    spark_config: dict, optional
        Dictionary of `Spark configuration options <https://spark.apache.org/docs/3.5.1/configuration.html#available-properties>`_ to use when creating a Spark session.
        Ignored unless `spark` is unspecified or `None`.
    kwargs
        Any keyword arguments are passed directly to the workflow
    """
    if not callable(workflow):
        workflow = _get_workflow(workflow)

    if spark is None:
        _builder = _SparkSession.builder.master("local")

        _conf = spark_config or dict()
        _logger.info("Creating SparkSession with config %s", _conf)

        for k, v in _conf.items():
            _builder = _builder.config(k, v)

        spark = _builder.getOrCreate()

    result = workflow(spark=spark, **kwargs)

    if spark is None:
        # If the caller did not pass in a Spark session, assume one was created.
        # To be a good citizen, clean it up now that we're done with it.
        # NOTE: if we begin returning a dataset object we should likely remove this code and place
        # the responsibility on the caller.
        result.data.sparkSession.stop()

    return result
