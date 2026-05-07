"""
Shared helpers for generic quant rollup backends.
"""

from collections.abc import (
    Callable as _Callable,
    Mapping as _Mapping,
    Sequence as _Sequence,
)
from typing import (
    Any as _Any,
    Union as _Union,
)

from pyspark.sql import (
    Column as _Column,
    DataFrame as _DataFrame,
    functions as _fns,
)
from wheely.mammoth import ConfidenceDataset as _ConfidenceDataset
from wheely.mammoth.semantics import (
    NORMALIZED_XIC_AREA as _NORMALIZED_XIC_AREA,
    XIC_AREA as _XIC_AREA,
)

_ReductionLike = _Union[str, _Callable[[_Column], _Column]]


def _resolve_reduction(
    reduction: _ReductionLike,
) -> _Callable[[_Column], _Column]:
    if callable(reduction):
        return reduction

    try:
        return {
            "sum": _fns.sum,
            "max": _fns.max,
            "min": _fns.min,
            "first": lambda col: _fns.first(col, ignorenulls=True),
        }[reduction]
    except KeyError as e:
        raise ValueError(f"Unsupported rollup reduction {reduction!r}") from e


def _normalize_group_key_columns(
    group_key_columns: _Sequence[str],
    *,
    label: str,
) -> list[str]:
    columns = list(group_key_columns)
    if not columns:
        raise ValueError(f"{label} must contain at least one column")
    if len(columns) != len(set(columns)):
        raise ValueError(f"{label} must not contain duplicate columns")
    return columns


def _normalize_rollup_axes(
    dataset: _Any,
    *,
    entity_key_columns: _Sequence[str],
    sample_column: str,
    feature_key_columns: _Sequence[str] | None = None,
    require_feature_keys: bool = True,
) -> tuple[list[str], list[str], list[str]]:
    entity_keys = _normalize_group_key_columns(
        entity_key_columns,
        label="entity_key_columns",
    )
    raw_feature_keys = (
        [] if feature_key_columns is None else list(feature_key_columns)
    )
    if raw_feature_keys:
        feature_keys = _normalize_group_key_columns(
            raw_feature_keys,
            label="feature_key_columns",
        )
    elif require_feature_keys:
        raise ValueError(
            "feature_key_columns must contain at least one column"
        )
    else:
        feature_keys = []

    if sample_column in entity_keys:
        raise ValueError(
            "sample_column must not also be present in entity_key_columns"
        )
    if sample_column in feature_keys:
        raise ValueError(
            "sample_column must not also be present in feature_key_columns"
        )

    data_columns = set(dataset.data.columns)
    if sample_column not in data_columns:
        raise ValueError(
            f"Dataset does not contain sample column {sample_column!r}"
        )

    required_columns = [*entity_keys, *feature_keys]
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in data_columns
    ]
    if missing_columns:
        raise ValueError(
            "Dataset is missing required rollup columns: "
            + ", ".join(repr(column_name) for column_name in missing_columns)
        )

    return entity_keys, feature_keys, [*entity_keys, sample_column]


def _normalize_intensity_column_map(
    dataset: _Any,
    intensity_columns: _Mapping[str, str] | _Sequence[str] | str | None = None,
) -> list[tuple[str, str]]:
    if intensity_columns is None:
        source_column = getattr(dataset, "intensity_column", None)
        if source_column is None:
            raise ValueError(
                "intensity_columns was not provided and dataset does not "
                "define intensity_column"
            )
        normalized = [(source_column, source_column)]
    elif isinstance(intensity_columns, str):
        normalized = [(intensity_columns, intensity_columns)]
    elif isinstance(intensity_columns, _Mapping):
        normalized = list(intensity_columns.items())
    else:
        normalized = [
            (column_name, column_name) for column_name in intensity_columns
        ]

    if not normalized:
        raise ValueError("At least one intensity column must be specified")

    seen_outputs = set()
    data_columns = set(dataset.data.columns)
    for source_column, output_column in normalized:
        if source_column not in data_columns:
            raise ValueError(
                f"Dataset does not contain intensity column {source_column!r}"
            )
        if output_column in seen_outputs:
            raise ValueError(
                "Intensity output columns must be unique. "
                f"Duplicate {output_column!r}."
            )
        seen_outputs.add(output_column)

    return normalized


def _resolve_rollup_output_intensity_columns(
    dataset: _Any,
    *,
    prefix: str = "",
) -> tuple[dict[str, str], str, dict[str, _Any]]:
    """
    Resolve source intensity tracks to output names for rollup wrappers.

    Naming is semantic-first, with a primary-column fallback:

    - the first intensity column annotated with ``XIC_AREA`` becomes
      ``{prefix}intensity``
    - the first intensity column annotated with ``NORMALIZED_XIC_AREA`` becomes
      ``{prefix}normalized_intensity``
    - if neither semantic is present, the dataset's primary intensity column
      becomes ``{prefix}intensity``
    - any remaining intensity columns keep their original name for unprefixed
      rollups, or gain ``prefix`` for prefixed rollups

    If an additional column would collide with an already-assigned output name,
    it is dropped. This intentionally favors the canonical semantic/primary
    names over preserving every conflicting auxiliary column.

    Returns
    -------
    intensity_columns : dict[str, str]
        Ordered mapping from source intensity column to rolled output column.
    intensity_column : str
        The primary output intensity column for the rolled dataset.
    semantics : dict[str, Any]
        Semantic annotations for rolled intensity columns only.
    """

    primary_column = getattr(dataset, "intensity_column", None)
    dataset_intensity_columns = getattr(dataset, "intensity_columns", None)
    if dataset_intensity_columns is None:
        intensity_columns = (
            [primary_column] if primary_column is not None else []
        )
    else:
        intensity_columns = list(dataset_intensity_columns)
        if (
            primary_column is not None
            and primary_column not in intensity_columns
        ):
            intensity_columns = [primary_column, *intensity_columns]

    if not intensity_columns:
        raise ValueError("Dataset must define at least one intensity column")

    semantics = getattr(dataset, "semantics", {})
    canonical_intensity_column = f"{prefix}intensity"
    canonical_normalized_column = f"{prefix}normalized_intensity"

    output_columns: dict[str, str] = {}
    output_semantics: dict[str, _Any] = {}
    used_outputs = set()
    used_sources = set()

    def _add_output(source_column: str, output_column: str) -> bool:
        if source_column in used_sources or output_column in used_outputs:
            return False

        output_columns[source_column] = output_column
        used_sources.add(source_column)
        used_outputs.add(output_column)

        if (semantic := semantics.get(source_column)) is not None:
            output_semantics[output_column] = semantic

        return True

    raw_column = next(
        (
            column_name
            for column_name in intensity_columns
            if semantics.get(column_name) == _XIC_AREA
        ),
        None,
    )
    if raw_column is not None:
        _add_output(raw_column, canonical_intensity_column)

    normalized_column = next(
        (
            column_name
            for column_name in intensity_columns
            if semantics.get(column_name) == _NORMALIZED_XIC_AREA
        ),
        None,
    )
    if normalized_column is not None:
        _add_output(normalized_column, canonical_normalized_column)

    if (
        raw_column is None
        and normalized_column is None
        and primary_column is not None
    ):
        _add_output(primary_column, canonical_intensity_column)
    elif primary_column is not None and primary_column not in used_sources:
        preserved_primary_column = (
            f"{prefix}{primary_column}" if prefix else primary_column
        )
        _add_output(primary_column, preserved_primary_column)

    for source_column in intensity_columns:
        if source_column in used_sources:
            continue
        preserved_output_column = (
            f"{prefix}{source_column}" if prefix else source_column
        )
        _add_output(source_column, preserved_output_column)

    primary_output_column = (
        canonical_intensity_column
        if canonical_intensity_column in used_outputs
        else (
            canonical_normalized_column
            if canonical_normalized_column in used_outputs
            else next(iter(output_columns.values()))
        )
    )

    return output_columns, primary_output_column, output_semantics


def _filter_rollup_dataset(
    dataset: _Any,
    *,
    qvalue_threshold: float | None = None,
    filter_column: str | _Column | None = None,
) -> _Any:
    if qvalue_threshold is not None:
        assert isinstance(
            dataset, _ConfidenceDataset
        ), "dataset must be a ConfidenceDataset if qvalue_threshold is specified"
        dataset = dataset.with_data(
            dataset.data.filter(dataset.qvalues <= qvalue_threshold),
        )

    if filter_column is not None:
        dataset = dataset.with_data(
            dataset.data.filter(filter_column),
        )

    return dataset


def _aggregate_reduced_columns(
    dataset: _Any,
    *,
    group_key_columns: _Sequence[str],
    column_reductions: _Mapping[str, _ReductionLike] | None = None,
) -> _DataFrame | None:
    aggregation_columns = _build_reduced_aggregation_columns(
        dataset,
        group_key_columns=group_key_columns,
        column_reductions=column_reductions,
    )
    if not aggregation_columns:
        return None

    return dataset.data.groupBy(*group_key_columns).agg(*aggregation_columns)


def _build_reduced_aggregation_columns(
    dataset: _Any,
    *,
    group_key_columns: _Sequence[str],
    column_reductions: _Mapping[str, _ReductionLike] | None = None,
) -> list[_Column]:
    if not column_reductions:
        return []

    data_columns = set(dataset.data.columns)
    aggregation_columns = []
    for column_name, reduction in column_reductions.items():
        if column_name not in data_columns:
            raise ValueError(
                f"Dataset does not contain preserved column {column_name!r}"
            )
        if column_name in group_key_columns:
            raise ValueError(
                f"Preserved column {column_name!r} is already a group key"
            )
        aggregation_columns.append(
            _resolve_reduction(reduction)(_fns.col(column_name)).alias(
                column_name
            )
        )

    return aggregation_columns


def _join_aggregates(
    left: _DataFrame,
    right: _DataFrame | None,
    *,
    join_columns: _Sequence[str],
) -> _DataFrame:
    if right is None:
        return left

    return left.join(right, on=list(join_columns), how="leftouter")
