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
    _build_reduced_aggregation_columns,
    _filter_rollup_dataset,
    _normalize_intensity_column_map,
    _normalize_rollup_axes,
    _resolve_reduction,
)


def roll_up_basic(
    dataset: _Any,
    *,
    entity_key_columns: _Sequence[str],
    sample_column: str,
    feature_key_columns: _Sequence[str] | None = None,
    intensity_columns: _Mapping[str, str] | _Sequence[str] | str | None = None,
    intensity_reduction: _ReductionLike = "sum",
    preserved_column_reductions: _Mapping[str, _ReductionLike] | None = None,
    qvalue_threshold: float | None = None,
    filter_column: str | _Column | None = None,
) -> _DataFrame:
    """
    Roll up one or more intensity tracks to a final output grain using a simple
    reduction such as ``sum`` or ``max``.

    This helper returns one row per ``(entity_key_columns, sample_column)``
    combination after any requested filtering is applied. ``feature_key_columns``
    define the lower-level feature identity that would be compared across
    samples by more advanced backends such as DirectLFQ. The basic backend does
    not use those feature keys in the reduction itself, but it accepts and
    validates them as part of the shared generic rollup contract so wrapper
    code can switch between backends without reshaping its arguments.

    To support multi-track rollups, ``intensity_columns`` may be either a
    single source column, a sequence of source columns, or a mapping from
    source column name to output column name. Each requested track is
    aggregated independently at the final output grain and emitted as a
    separate column in the returned :py:class:`pyspark.sql.DataFrame`.

    Parameters
    ----------
    dataset
        Input dataset containing the source intensities and all grouping
        columns. If ``qvalue_threshold`` is specified, this must be a
        :py:class:`ConfidenceDataset`.
    entity_key_columns
        Columns identifying the final rollup entity, excluding the sample axis.
        The returned frame will contain one row per
        ``(entity_key_columns, sample_column)`` pair.
    sample_column
        Column identifying sample membership in the input dataset and output
        frame. This column must not also appear in ``entity_key_columns`` or
        ``feature_key_columns``.
    feature_key_columns
        Optional columns identifying the same lower-level feature across
        samples within each final entity. These keys are accepted as part of
        the shared generic rollup contract. When provided they are validated,
        but they do not affect the basic reduction backend.
    intensity_columns
        Source intensity column or columns to aggregate. When a mapping is
        provided, keys are source column names and values are output column
        names. If omitted, ``dataset.intensity_column`` is used and the source
        column name is preserved.
    intensity_reduction
        Reduction applied independently to each requested intensity track at
        the final output grain. Supported string values are ``"sum"``,
        ``"max"``, ``"min"``, and ``"first"``, or a callable returning a
        Spark aggregation expression.
    preserved_column_reductions
        Optional mapping of non-key input columns to reduction names or
        callables. Each preserved column is reduced at the same
        ``(entity_key_columns, sample_column)`` grain and included in the
        returned frame.
    qvalue_threshold
        Optional confidence threshold applied before rollup. When provided,
        only rows with ``dataset.qvalues <= qvalue_threshold`` are retained.
    filter_column
        Optional additional Spark filter applied before rollup. May be either a
        column name or a Spark boolean expression.

    Returns
    -------
    DataFrame
        A Spark DataFrame containing one row per
        ``(entity_key_columns, sample_column)`` pair, with one aggregated
        output column per requested intensity track and any requested preserved
        columns.
    """
    filtered = _filter_rollup_dataset(
        dataset,
        qvalue_threshold=qvalue_threshold,
        filter_column=filter_column,
    )
    _entity_keys, _feature_keys, output_group_keys = _normalize_rollup_axes(
        filtered,
        entity_key_columns=entity_key_columns,
        sample_column=sample_column,
        feature_key_columns=feature_key_columns,
        require_feature_keys=False,
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
    preserved_aggregations = _build_reduced_aggregation_columns(
        filtered,
        group_key_columns=output_group_keys,
        column_reductions=preserved_column_reductions,
    )
    return filtered.data.groupBy(*output_group_keys).agg(
        *intensity_aggregations,
        *preserved_aggregations,
    )
