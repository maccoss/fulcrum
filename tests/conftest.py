"""
`conftest.py`: common configuration and fixtures for all unit tests
"""

import logging

import json

import pytest

from pyspark.sql import SparkSession


@pytest.fixture
def param_dict():
    """
    Mock parameters in a dict
    """
    return dict(
        search=dict(
            backend="read_existing",
            engine="encyclopedia",
            location="data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
        ),
        airpot=dict(
            # subset_size=1024,
            # test_fdr=0.05,
        ),
    )


@pytest.fixture
def param_json(param_dict):
    return json.dumps(param_dict)


@pytest.fixture
def param_json_file(param_dict, tmp_path):
    path = tmp_path / "mock_params.json"
    with open(path, "w") as f:
        json.dump(param_dict, f)

    return path


@pytest.fixture
def param_toml(param_dict):
    toml = pytest.importorskip("toml")

    return toml.dumps(param_dict)


@pytest.fixture
def param_toml_file(param_dict, tmp_path):
    toml = pytest.importorskip("toml")

    path = tmp_path / "mock_params.toml"
    with open(path, "w") as f:
        toml.dump(param_dict, f)

    return path


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (
        SparkSession.builder.master("local[2]")
        # .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:3.0.1')
        .appName("pytest-pyspark-local-testing")
        # .enableHiveSupport()
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark
