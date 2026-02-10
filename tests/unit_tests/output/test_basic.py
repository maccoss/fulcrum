"""
Tests for the basic output functionality
"""

import pytest
from typing import Union

from wheely.mammoth import PsmDataset, ConfidenceDataset
from wheely.mammoth.proteins import ProteinDataset, ProteinConfidenceDataset

from fulcrum.output.basic import write_csv, write_parquet


@pytest.fixture(
    params=[
        "psm_dataset",
        "confidence_dataset",
        "protein_dataset",
        "protein_confidence_dataset",
    ]
)
def dataset(
    request,
) -> Union[
    PsmDataset, ConfidenceDataset, ProteinDataset, ProteinConfidenceDataset
]:
    return request.getfixturevalue(request.param)


def test_write_csv(tmp_path, dataset):
    """
    Test that writing CSV results is successful.
    """
    loc = str(tmp_path / "test-output")

    write_csv(
        dataset,
        location=loc,
        header=True,
        include_decoys=True,  # disable filtering
    )

    read = dataset.data.sparkSession.read.csv(
        loc,
        header=True,
    )

    assert read.count() == dataset.data.count()
    assert all(c in read.columns for c in dataset.columns)

    # TODO: assertions


def test_write_parquet(tmp_path, dataset):
    """
    Test that writing CSV results is successful.
    """
    loc = str(tmp_path / "test-output")

    write_parquet(
        dataset,
        location=loc,
        include_decoys=True,  # disable filtering
    )

    read = dataset.data.sparkSession.read.parquet(loc)

    assert read.count() == dataset.data.count()
    assert all(c in read.columns for c in dataset.columns)

    # TODO: assertions
