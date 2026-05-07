import logging

import pytest
from pyspark.sql import SparkSession

from wheely.mammoth.dataset import PsmIntensityConfidenceDataset

from fulcrum.quant.protein.basic import quantify_proteins_basic
from fulcrum.quant.rollup import (
    roll_up_basic,
    roll_up_directlfq,
)


@pytest.fixture(scope="session")
def spark_session(request):
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-rollup")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())
    logging.getLogger("py4j").setLevel(logging.WARN)
    return spark


@pytest.fixture
def intensity_confidence_dataset(
    spark_session,
) -> PsmIntensityConfidenceDataset:
    data = spark_session.createDataFrame(
        [
            ("s1", "P1", "pepA", 2, True, 0.02, 100.0, 10.0, 1.0),
            ("s1", "P1", "pepA", 3, True, 0.01, 100.0, 5.0, 0.5),
            ("s1", "P2", "pepB", 2, False, 0.20, 200.0, 4.0, 0.4),
            ("s2", "P1", "pepA", 2, True, 0.03, 100.0, 8.0, 0.8),
            ("s2", "P1", "pepC", 2, True, 0.05, 150.0, 6.0, 0.6),
            ("s2", "P1", "pepC", 3, True, 0.04, 150.0, 2.0, 0.2),
        ],
        schema=[
            "sample",
            "protein_group",
            "peptide",
            "charge",
            "target",
            "qvalue",
            "mass",
            "raw_intensity",
            "normalized_intensity",
        ],
    )

    return PsmIntensityConfidenceDataset(
        data,
        sample_column="sample",
        target_column="target",
        qvalue_column="qvalue",
        intensity_column="normalized_intensity",
        intensity_columns=["normalized_intensity", "raw_intensity"],
        score_columns=[],
        peptide_column="peptide",
        charge_column="charge",
        protein_column="protein_group",
        protein_delim=";",
        spectrum_columns=[],
    )


def test_roll_up_basic_supports_multiple_tracks_and_preserved_reductions(
    intensity_confidence_dataset,
):
    rolled = roll_up_basic(
        intensity_confidence_dataset,
        entity_key_columns=["peptide"],
        sample_column="sample",
        feature_key_columns=None,
        intensity_columns={
            "raw_intensity": "peptide_raw",
            "normalized_intensity": "peptide_normalized",
        },
        intensity_reduction="sum",
        preserved_column_reductions={
            "protein_group": "first",
            "target": "max",
            "qvalue": "min",
            "mass": "first",
        },
    )

    rows = {
        (row["sample"], row["peptide"]): row.asDict()
        for row in rolled.collect()
    }

    assert rows[("s1", "pepA")]["peptide_raw"] == pytest.approx(15.0)
    assert rows[("s1", "pepA")]["peptide_normalized"] == pytest.approx(1.5)
    assert rows[("s1", "pepA")]["protein_group"] == "P1"
    assert rows[("s1", "pepA")]["qvalue"] == pytest.approx(0.01)
    assert rows[("s1", "pepA")]["mass"] == pytest.approx(100.0)
    assert rows[("s1", "pepA")]["target"] is True

    assert rows[("s2", "pepC")]["peptide_raw"] == pytest.approx(8.0)
    assert rows[("s2", "pepC")]["peptide_normalized"] == pytest.approx(0.8)
    assert rows[("s2", "pepC")]["qvalue"] == pytest.approx(0.04)


def test_quantify_proteins_basic_keeps_legacy_surface(
    intensity_confidence_dataset,
):
    rolled = quantify_proteins_basic(
        intensity_confidence_dataset,
        qvalue_threshold=0.05,
    )

    rows = {
        (row["sample"], row["protein_group"]): row.asDict()
        for row in rolled.data.collect()
    }

    assert set(rolled.columns) == {
        "protein_group",
        "sample",
        "target",
        "intensity",
    }
    assert rows[("s1", "P1")]["intensity"] == pytest.approx(1.5)
    assert rows[("s2", "P1")]["intensity"] == pytest.approx(1.6)
    assert rows[("s1", "P1")]["target"] is True
    assert ("s1", "P2") not in rows


def test_roll_up_directlfq_supports_multiple_tracks_and_preserved_reductions(
    intensity_confidence_dataset,
):
    pytest.importorskip("directlfq")

    rolled = roll_up_directlfq(
        intensity_confidence_dataset,
        entity_key_columns=["protein_group"],
        sample_column="sample",
        feature_key_columns=["peptide", "charge"],
        intensity_columns={
            "raw_intensity": "protein_raw",
            "normalized_intensity": "protein_normalized",
        },
        preserved_column_reductions={
            "target": "max",
            "qvalue": "min",
        },
        qvalue_threshold=0.05,
    )

    rows = {
        (row["sample"], row["protein_group"]): row.asDict()
        for row in rolled.collect()
    }

    assert set(rows) == {("s1", "P1"), ("s2", "P1")}
    assert rows[("s1", "P1")]["qvalue"] == pytest.approx(0.01)
    assert rows[("s2", "P1")]["qvalue"] == pytest.approx(0.03)
    assert rows[("s1", "P1")]["target"] is True
    assert rows[("s2", "P1")]["target"] is True
    assert rows[("s1", "P1")]["protein_raw"] > 0
    assert rows[("s1", "P1")]["protein_normalized"] > 0
    assert rows[("s2", "P1")]["protein_raw"] > 0
    assert rows[("s2", "P1")]["protein_normalized"] > 0


@pytest.mark.parametrize("feature_key_columns", [None, []])
def test_roll_up_directlfq_requires_feature_keys(
    intensity_confidence_dataset,
    feature_key_columns,
):
    with pytest.raises(
        ValueError,
        match="feature_key_columns must contain at least one column",
    ):
        roll_up_directlfq(
            intensity_confidence_dataset,
            entity_key_columns=["protein_group"],
            sample_column="sample",
            feature_key_columns=feature_key_columns,
            intensity_columns={
                "raw_intensity": "protein_raw",
            },
        )
