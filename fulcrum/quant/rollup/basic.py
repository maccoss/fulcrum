"""
Generic rollup helpers for quantification backends.
"""

from collections.abc import (
    Mapping as _Mapping,
    Sequence as _Sequence,
)
from typing import Any as _Any

from pyspark.sql import (
    Column as _Column,
    DataFrame as _DataFrame,
    functions as _fns,
)

from .utils import (
    _ReductionLike,
    _aggregate_reduced_columns,
    _filter_rollup_dataset,
    _join_aggregates,
    _normalize_group_key_columns,
    _normalize_intensity_column_map,
    _resolve_reduction,
)


def roll_up_basic(
    dataset: _Any,
    *,
    group_key_columns: _Sequence[str],
    intensity_columns: _Mapping[str, str] | _Sequence[str] | str | None = None,
    intensity_reduction: _ReductionLike = "sum",
    preserved_column_reductions: _Mapping[str, _ReductionLike] | None = None,
    qvalue_threshold: float | None = None,
    filter_column: str | _Column | None = None,
) -> _DataFrame:
    filtered = _filter_rollup_dataset(
        dataset,
        qvalue_threshold=qvalue_threshold,
        filter_column=filter_column,
    )
    group_keys = _normalize_group_key_columns(
        group_key_columns,
        label="group_key_columns",
    )
    intensity_column_map = _normalize_intensity_column_map(
        filtered,
        intensity_columns,
    )

    reduction_fn = _resolve_reduction(intensity_reduction)
    intensity_aggregations = [
        reduction_fn(_fns.col(source_column)).alias(output_column)
        for source_column, output_column in intensity_column_map
    ]

    intensities = filtered.data.groupBy(*group_keys).agg(
        *intensity_aggregations
    )
    preserved = _aggregate_reduced_columns(
        filtered,
        group_key_columns=group_keys,
        column_reductions=preserved_column_reductions,
    )

    return _join_aggregates(
        intensities,
        preserved,
        join_columns=group_keys,
    )
