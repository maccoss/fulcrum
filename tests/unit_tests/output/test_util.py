import pytest

import pyspark.sql.functions as fns

from scry.output.util import filter_psms

from ...conftest import rand
from .library.test_write import spectra_dataset


@pytest.fixture(params=[True, False])
def include_decoys(request):
    return request.param


def test_filter_psms_with_confidence_dataset(
    confidence_dataset, include_decoys
):
    # Test data
    threshold_col = None
    qval_thresh = 0.01

    # Call the function
    filtered_dataset = filter_psms(
        confidence_dataset,
        threshold_col,
        qval_thresh,
        include_decoys=include_decoys,
    )

    # Perform assertions (e.g., check if the filtered dataset contains the expected PSMs)
    assert confidence_dataset.data.count() >= filtered_dataset.data.count()
    assert filtered_dataset.data.filter(
        filtered_dataset.qvalues > qval_thresh
    ).isEmpty()
    if not include_decoys:
        assert filtered_dataset.data.filter(
            ~filtered_dataset.targets
        ).isEmpty()


def test_filter_psms_with_psm_dataset(psm_dataset):
    # Test data
    threshold_col = "threshold"
    psm_dataset = psm_dataset.with_data(
        psm_dataset.data.withColumn(threshold_col, rand() >= 0.5)
    )

    # These will be ignored
    qval_thresh = None
    include_decoys = None

    # Call the function
    filtered_dataset = filter_psms(
        psm_dataset, threshold_col, qval_thresh, include_decoys
    )

    # Perform assertions (e.g., check if the filtered dataset contains the expected PSMs)
    assert psm_dataset.data.count() >= filtered_dataset.data.count()
    assert filtered_dataset.data.filter(~fns.col(threshold_col)).isEmpty()


# Because the dataset lacks confidence estimates, the qval_thresh will be ignored
@pytest.mark.parametrize("qval_thresh", [None, 0.1])
def test_filter_decoys_only(psm_dataset, qval_thresh):
    """
    Test that we can filter a non-confidence dataset based on decoy/target only
    """
    threshold_col = None
    include_decoys = False

    filtered_dataset = filter_psms(
        psm_dataset, threshold_col, qval_thresh, include_decoys
    )

    # Perform assertions (e.g., check if the filtered dataset contains the expected PSMs)
    assert psm_dataset.data.count() >= filtered_dataset.data.count()
    assert filtered_dataset.data.filter(~filtered_dataset.targets).isEmpty()


@pytest.mark.parametrize(
    "dataset_fixture", ["psm_dataset", "confidence_dataset", "spectra_dataset"]
)
def test_filter_psms_no_filtering(request, dataset_fixture):
    """
    Runs on both dataset fixtures to check the same logic applies to both kinds of dataset.
    """
    dataset = request.getfixturevalue(dataset_fixture)

    # Test data
    threshold_col = None
    qval_thresh = None
    include_decoys = True

    # Call the function
    filtered_dataset = filter_psms(
        dataset, threshold_col, qval_thresh, include_decoys=include_decoys
    )

    # Perform assertions (e.g., check if the filtered dataset is the same as the original dataset)
    assert dataset.data.count() == filtered_dataset.data.count()
