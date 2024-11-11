"""
`conftest.py`: common configuration and fixtures for all unit tests
"""

import logging

import json

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand as _rand

from wheely.mammoth import ConfidenceDataset, PsmDataset
from wheely.mammoth.proteins import ProteinDataset, ProteinConfidenceDataset
from wheely.mammoth.parsers import read_encyclopedia_features

from cortado.protein import score_proteins


@pytest.fixture
def param_dict():
    """
    Mock parameters in a dict
    """
    return dict(
        search=dict(
            backend="read_existing",
            engine="encyclopedia",  # Note: provided by wheely-mammoth plugin
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


def rand(seed=0):
    """
    Force specifying a seed to aid repeatability of tests.
    """
    return _rand(seed=seed)


@pytest.fixture
def psm_dataset() -> PsmDataset:
    # Create a PsmDataset fixture
    dataset = read_encyclopedia_features(
        "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
    )

    assert dataset.peptide_column in dataset.columns
    assert not [c for c in dataset.columns if c not in dataset.data.columns]

    return dataset


@pytest.fixture
def confidence_dataset(psm_dataset) -> ConfidenceDataset:
    # Create a ConfidenceDataset fixture
    dataset = ConfidenceDataset(
        psm_dataset.data.withColumn("q-value", rand()),
        qvalue_column="q-value",
        target_column=psm_dataset.target_column,
        spectrum_columns=psm_dataset.spectrum_columns,
        score_columns=psm_dataset.score_columns,
        peptide_column=psm_dataset.peptide_column,
        protein_column=psm_dataset.protein_column,
        protein_delim=psm_dataset.protein_delim,
    )

    assert all(
        dataset.data.groupBy(dataset.targets).count().toPandas()["count"] > 0
    )

    return dataset


@pytest.fixture
def protein_dataset(psm_dataset) -> ProteinDataset:
    return score_proteins(
        psm_dataset,
        rollup_level="peptide",
        score_column=psm_dataset.score_columns[0],
        desc=False,
        assign_confidence=False,
    )


@pytest.fixture
def protein_confidence_dataset(psm_dataset) -> ProteinConfidenceDataset:
    return score_proteins(
        psm_dataset,
        rollup_level="peptide",
        score_column=psm_dataset.score_columns[0],
        desc=False,
        assign_confidence=True,
    )
