import pytest

from pyspark.sql.functions import col

from wheely.mammoth import ConfidenceDataset, PsmDataset

from scry.output.library.write import _filter_psms


@pytest.fixture
def confidence_dataset():
    # Create a ConfidenceDataset fixture
    dataset = ConfidenceDataset(...)  # TODO
    yield dataset


@pytest.fixture
def psm_dataset():
    # Create a PsmDataset fixture
    dataset = PsmDataset(...)  # TODO
    yield dataset


def test_filter_psms_with_confidence_dataset(confidence_dataset):
    # Test data
    threshold_col = None
    qval_thresh = 0.01

    # Call the function
    filtered_dataset = _filter_psms(
        confidence_dataset, threshold_col, qval_thresh
    )

    # Perform assertions (e.g., check if the filtered dataset contains the expected PSMs)
    assert confidence_dataset.data.count() > filtered_dataset.data.count()
    assert filtered_dataset.data.filter(
        filtered_dataset.qvalues > qval_thresh
    ).isEmpty()


def test_filter_psms_with_confidence_dataset_missing_qval_thresh(
    confidence_dataset,
):
    # Test data
    threshold_col = None
    qval_thresh = 0.01  # ignored

    # Call the function and assert that it raises a ValueError for missing qval_thresh
    with pytest.raises(ValueError):
        _filter_psms(confidence_dataset, threshold_col, qval_thresh)


def test_filter_psms_with_psm_dataset(psm_dataset):
    # Test data
    threshold_col = "threshold"
    qval_thresh = None

    # Call the function
    filtered_dataset = _filter_psms(psm_dataset, threshold_col, qval_thresh)

    # Perform assertions (e.g., check if the filtered dataset contains the expected PSMs)
    assert confidence_dataset.data.count() > filtered_dataset.data.count()
    assert filtered_dataset.data.filter(~col(threshold_col)).isEmpty()


@pytest.mark.parametrize(
    "dataset_fixture", ["psm_dataset", "confidence_dataset"]
)
def test_filter_psms_no_filtering(request, dataset_fixture):
    """
    Runs on both dataset fixtures to check the same logic applies to both kinds of dataset.
    """
    dataset = request.getfixturevalue(dataset_fixture)

    # Test data
    threshold_col = None
    qval_thresh = None

    # Call the function
    filtered_dataset = _filter_psms(dataset, threshold_col, qval_thresh)

    # Perform assertions (e.g., check if the filtered dataset is the same as the original dataset)
    assert confidence_dataset.data.count() == filtered_dataset.data.count()
