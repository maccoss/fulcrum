import numpy as np
from pyspark.sql import functions as fns

import pytest

from wheely.mammoth.dataset import PsmIntensityConfidenceDataset

from fulcrum.quant.normalization.util import *


@pytest.mark.parametrize("filter_qvals", [True, False])
@pytest.mark.parametrize("include_decoys", [True, False])
def test_get_filtered_intensities(spark_session, filter_qvals, include_decoys):
    """
    Test that intensity column filtering is correct.
    """

    dset = PsmIntensityConfidenceDataset(
        spark_session.createDataFrame(
            [
                (True, 0.001, 8),
                (True, 0.1, 4),
                (False, 0.001, 2),
                (False, 0.1, 1),
            ],
            schema=[
                "target",
                "qvalue",
                "intensity",
            ],
        ),
        target_column="target",
        qvalue_column="qvalue",
        intensity_column="intensity",
        sample_column="__fake",
        score_columns=[],
        peptide_column="__fake",
        spectrum_columns=[],
    )

    expected = {
        (True, True): 10,  # 8 + 2
        (True, False): 8,
        (False, True): 15,  # 8 + 4 + 2 + 1
        (False, False): 12,  # 8 + 4
    }

    kwargs = dict(
        include_decoys=include_decoys,
    )

    if filter_qvals:
        kwargs["qval_thresh"] = 0.01

    col = get_filtered_intensities(dset, **kwargs)

    res = (
        dset.data.select(
            fns.sum(col.alias("inten_filt")),
        )
        .toPandas()
        .iloc[0, 0]
    )

    assert res == expected[(filter_qvals, include_decoys)]
