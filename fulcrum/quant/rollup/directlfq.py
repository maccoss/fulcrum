"""
Generic rollup helpers for quantification backends.
"""

import logging as _logging
from collections.abc import (
    Mapping as _Mapping,
    Sequence as _Sequence,
)
from typing import Any as _Any

import numpy as _np
import pandas as _pd
from pyspark.sql import (
    Column as _Column,
    DataFrame as _DataFrame,
)
from pyspark.sql.types import (
    DoubleType as _DoubleType,
    StructField as _StructField,
    StructType as _StructType,
)

_logger = _logging.getLogger(__name__)

from .utils import (
    _ReductionLike,
    _aggregate_reduced_columns,
    _filter_rollup_dataset,
    _join_aggregates,
    _normalize_group_key_columns,
    _normalize_intensity_column_map,
)


def _build_directlfq_schema(
    dataset: _Any,
    *,
    final_group_key_columns: _Sequence[str],
    intensity_output_column: str,
) -> _StructType:
    fields = [
        _StructField(
            column_name,
            dataset.data.schema[column_name].dataType,
            nullable=True,
        )
        for column_name in final_group_key_columns
    ]
    fields.append(
        _StructField(
            intensity_output_column,
            _DoubleType(),
            nullable=True,
        )
    )
    return _StructType(fields)


def _make_quant_id(
    pdf: _pd.DataFrame,
    quant_key_columns: _Sequence[str],
) -> _pd.Series:
    columns = list(quant_key_columns)
    if not columns:
        raise ValueError("quant_key_columns must contain at least one column")

    if len(columns) == 1:
        return pdf[columns[0]].astype(str)

    return (
        pdf[columns]
        .fillna("")
        .astype(str)
        .agg(lambda values: "\x1f".join(values), axis=1)
    )


def _estimate_directlfq_partition(
    pdf: _pd.DataFrame,
    *,
    partition_key_columns: _Sequence[str],
    sample_column: str,
    quant_key_columns: _Sequence[str],
    intensity_column: str,
    intensity_output_column: str,
    directlfq_log_level: int,
) -> _pd.DataFrame:
    result_columns = [
        *partition_key_columns,
        sample_column,
        intensity_output_column,
    ]

    if pdf.empty:
        return _pd.DataFrame(columns=result_columns)

    quant_id = _make_quant_id(pdf, quant_key_columns)
    pdf = pdf.assign(__entity_id="entity", __quant_id=quant_id)

    wide = pdf.pivot_table(
        index=["__entity_id", "__quant_id"],
        columns=sample_column,
        values=intensity_column,
        aggfunc="first",
        fill_value=None,
    )

    if wide.empty:
        return _pd.DataFrame(columns=result_columns)

    wide.replace(0, _np.nan, inplace=True)

    wide_is_neg = wide < 0
    if (wide_is_neg).any().any():
        _logger.warning(
            "Negative intensity values found for %s",
            {
                column_name: pdf[column_name].iloc[0]
                for column_name in partition_key_columns
            },
        )
        wide[wide_is_neg] = _np.nan

    if wide.notna().sum().sum() == 0:
        return _pd.DataFrame(columns=result_columns)

    from directlfq import config as _lfq_config

    _lfq_config.setup_logging = lambda *_, **__: ()
    _lfq_config.check_wether_to_copy_numpy_arrays_derived_from_pandas()

    if not _lfq_config.COPY_NUMPY_ARRAYS_DERIVED_FROM_PANDAS:
        if not wide.to_numpy(copy=False).flags.writeable:
            _lfq_config.COPY_NUMPY_ARRAYS_DERIVED_FROM_PANDAS = True

    from directlfq.protein_intensity_estimation import (
        estimate_protein_intensities,
    )

    _logging.getLogger("directlfq").setLevel(directlfq_log_level)

    wide = _np.log2(wide)
    wide.index.set_names(
        [_lfq_config.PROTEIN_ID, _lfq_config.QUANT_ID],
        inplace=True,
    )
    _lfq_config.set_compile_normalized_ion_table(False)

    protein_df, _ = estimate_protein_intensities(
        wide,
        min_nonan=1,
        num_samples_quadratic=10,
        num_cores=1,
    )

    if protein_df.empty:
        return _pd.DataFrame(columns=result_columns)

    protein_df.rename(
        columns={_lfq_config.PROTEIN_ID: "__entity_id"},
        inplace=True,
    )

    protein_long = (
        protein_df.set_index("__entity_id")
        .stack()
        .reset_index()
        .rename(columns={"level_1": sample_column, 0: intensity_output_column})
        .drop(columns="__entity_id")
    )

    for column_name in partition_key_columns:
        protein_long[column_name] = pdf[column_name].iloc[0]

    return protein_long[result_columns]


def roll_up_directlfq(
    dataset: _Any,
    *,
    partition_key_columns: _Sequence[str],
    sample_column: str,
    quant_key_columns: _Sequence[str],
    intensity_columns: _Mapping[str, str] | _Sequence[str] | str | None = None,
    preserved_column_reductions: _Mapping[str, _ReductionLike] | None = None,
    qvalue_threshold: float | None = None,
    filter_column: str | _Column | None = None,
) -> _DataFrame:
    filtered = _filter_rollup_dataset(
        dataset,
        qvalue_threshold=qvalue_threshold,
        filter_column=filter_column,
    )
    partition_keys = _normalize_group_key_columns(
        partition_key_columns,
        label="partition_key_columns",
    )
    if sample_column in partition_keys:
        raise ValueError(
            "sample_column must not also be present in partition_key_columns"
        )

    if sample_column not in filtered.data.columns:
        raise ValueError(
            f"Dataset does not contain sample column {sample_column!r}"
        )

    quant_keys = _normalize_group_key_columns(
        quant_key_columns,
        label="quant_key_columns",
    )
    intensity_column_map = _normalize_intensity_column_map(
        filtered,
        intensity_columns,
    )
    final_group_keys = [*partition_keys, sample_column]
    directlfq_log_level = _logging.getLogger("directlfq").getEffectiveLevel()

    intensity_frames = []
    for source_column, output_column in intensity_column_map:
        schema = _build_directlfq_schema(
            filtered,
            final_group_key_columns=final_group_keys,
            intensity_output_column=output_column,
        )
        intensity_frames.append(
            filtered.data.groupBy(*partition_keys).applyInPandas(
                lambda pdf: _estimate_directlfq_partition(
                    pdf,
                    partition_key_columns=partition_keys,
                    sample_column=sample_column,
                    quant_key_columns=quant_keys,
                    intensity_column=source_column,
                    intensity_output_column=output_column,
                    directlfq_log_level=directlfq_log_level,
                ),
                schema,
            )
        )

    intensities = intensity_frames[0]
    for frame in intensity_frames[1:]:
        intensities = intensities.join(
            frame,
            on=final_group_keys,
            how="outer",
        )

    preserved = _aggregate_reduced_columns(
        filtered,
        group_key_columns=final_group_keys,
        column_reductions=preserved_column_reductions,
    )

    return _join_aggregates(
        intensities,
        preserved,
        join_columns=final_group_keys,
    )
