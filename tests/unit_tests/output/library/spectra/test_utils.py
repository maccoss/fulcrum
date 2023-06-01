import pandas as pd
import numpy as np
import pytest
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType
from pyspark.sql.functions import col

from scry.output.library.spectra.utils import (
    lists_to_peaklist,
    rows_to_peaklist,
)

# Test data
mz_values = [100.0, 101.0, 102.0]
intensity_values = [0.5, 0.8, 0.6]
mz_series = pd.Series(mz_values)
intensity_series = pd.Series(intensity_values)


def test_lists_to_peaklist(spark_session):
    schema = StructType(
        [
            StructField("mz", ArrayType(DoubleType()), nullable=False),
            StructField("intensity", ArrayType(DoubleType()), nullable=False),
        ]
    )

    df = spark_session.createDataFrame(
        pd.DataFrame(
            [
                [mz_values, intensity_values],
            ]
        ),
        schema=schema,
    )

    result = (
        df.select(lists_to_peaklist(col("mz"), col("intensity")))
        .toPandas()
        .values
    )

    assert result == [list(zip(mz_values, intensity_values))]


def test_rows_to_peaklist(spark_session):
    schema = StructType(
        [
            StructField("mz", DoubleType(), nullable=False),
            StructField("intensity", DoubleType(), nullable=False),
        ]
    )
    df = spark_session.createDataFrame(
        pd.DataFrame(list(zip(mz_values, intensity_values))), schema=schema
    )

    # Apply the Pandas UDF
    udf_result = df.groupby().agg(rows_to_peaklist("mz", "intensity"))

    # Convert the result back to a regular Pandas DataFrame for easier comparison
    result_df = udf_result.toPandas()

    expected_result = [(100, 0.5), (101, 0.8), (102, 0.6)]
    assert result_df.iloc[0, 0] == expected_result
