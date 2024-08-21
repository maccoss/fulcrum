"""
`scry.output.confidence`: output implementations for confidence results
"""

from wheely.mammoth import PsmDataset as _PsmDataset


def write_csv(
        data: _PsmDataset,
        location: str,
        **kwargs
):
    """
    Write the given dataset to CSV. A thin wrapper around `pyspark.sql.DataFrameWrite.csv`.

    Parameters
    ----------
    dataset: ConfidenceDataset
        The dataset to write
    location: A string specifying where the results should be written
    **kwargs: additional keyword arguments to pass to `pyspark.sql.DataFrameWrite.csv`.
              Defaults: `{"mode": "errorifexists", "header": True}`
    """
    data.data.write.csv(
        path=location,
        mode=kwargs.pop("mode", "errorifexists"),
        header=kwargs.pop("header", True),
    )
