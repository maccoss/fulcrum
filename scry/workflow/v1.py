"""
`scry.workflows.v1`: Basic ID and quantification workflow packaged in Scry v1
"""

import logging as _logging
from time import time as _time
from typing import (
    Any as _Any,
    Dict as _Dict,
    Optional as _Optional,
    Tuple as _Tuple,
    Union as _Union,
)

import polars as _pl
from pyspark.sql import (
    SparkSession as _SparkSession,
    functions as _fns,
)

from airpot import (
    brew as _brew,
    BrewResult as _BrewResult,
)
from cortado import assign_confidence as _assign_confidence
from cortado.protein import score_proteins as _score_proteins
from proffer.spark import infer_spark as _infer_spark
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
)
from wheely.mammoth.proteins import (
    ProteinConfidenceDataset as _ProteinConfidenceDataset,
    ProteinDataset as _ProteinDataset,
)
from wheely.mammoth.semantics import (
    PROTEOTYPIC_PEPTIDE as _PROTEOTYPIC_PEPTIDE,
)
from ..quant.protein.util import (
    merge_protein_confidence_and_quant as _merge_protein_confidence_and_quant,
)

from ..search import (
    get_backend as _get_search_backend,
)
from ..quant.peptide import get_backend as _get_peptide_quant_backend
from ..quant.protein import get_backend as _get_protein_rollup_backend
from ..output import (
    get_backend as _get_output_backend,
)

_logger = _logging.getLogger(__name__)


def scry_v1(
    search: _Optional[_Dict[str, _Any]] = None,
    airpot: _Optional[_Dict[str, _Any]] = None,
    cortado: _Optional[_Dict[str, _Any]] = None,
    peptide_quant: _Optional[_Dict[str, _Any]] = None,
    proffer: _Optional[_Dict[str, _Any]] = None,
    protein_scoring: _Optional[_Dict[str, _Any]] = None,
    protein_rollup: _Optional[_Dict[str, _Any]] = None,
    output: _Optional[_Dict[str, _Any]] = None,
    peptide_output: _Optional[_Union[str, _Dict[str, _Any]]] = None,
    protein_output: _Optional[_Union[str, _Dict[str, _Any]]] = None,
    spark: _Optional[_SparkSession] = None,
) -> _Tuple[_PsmDataset, _ProteinDataset]:
    """
    Workflow for peptide and protein ID/quant

    Most steps of the workflow can use a variety of implementations, and support
    plugins to permit maximum flexibility.

    To use, call ``scry.scry(workflow="v1")`` or run ``scry`` using the following TOML:

    .. code:: toml

        workflow = "v1"

    Parameters
    ----------
    search : dict, optional
        Specifies the backend and arguments that will give intial putative PSMs.

        Include a ``backend`` from the :py:mod:`scry.search` registry (default: :py:func:`~scry.search.read_existing`)
        or a suitable ``callable``.

        Most backends will accept a ``location`` as a string or list of strings giving the
        location(s) of files.

    airpot : dict, optional
        Any arguments to pass to :py:mod:`airpot` for rescoring search results.

        For more information, see `airpot <https://github.com/seerbio/airpot>`_.

    cortado : dict, optional
        Any arguments to pass to :py:mod:`cortado` for confidence estimation on the rescored dataset.

        If ``score_column`` is unspecified or ``None``, uses the first score returned by ``airpot``.
        Defaults to treating larger scores are better; this can be overridden with ``desc=True``.

        For more information, see `cortado <https://github.com/seerbio/cortado>`_.

    peptide_quant : dict, optional,
        Specifies the backend and arguments to use for computing PSM quantities.

        Include a ``backend`` from the :py:mod:`scry.quant.peptide` registry (default: :py:func:`~scry.quant.peptide.basic`)
        or a suitable ``callable``.

    proffer : dict, optional
        Any arguments to pass to :py:mod:`proffer`'s ``infer_spark()`` function.

        In typical usage, it's strongly recommended to specify a ``qvalue_threshold`` to avoid
        allow low-confidence results to affect protein inference.

        For more information, see `proffer <https://github.com/seerbio/proffer>`_.

    protein_scoring : dict, optional
        Any arguments to :py:func:`cortado.protein.scoring.score_proteins` for protein scoring.

        For more information, see `cortado's documentation <https://github.com/seerbio/cortado/blob/main/cortado/protein/scoring.py>`_.

    protein_rollup : dict, optional
        Specifies the backend and arguments to use for computing protein quantities.

        Include a ``backend`` from the :py:mod:`scry.quant.protein` registry (default: :py:func:`~scry.quant.protein.basic`)
        or a suitable ``callable``.

    output : dict, optional
        Default parameters for outputting both peptide and protein level results. Parameters given here will
        be overridden by the ``peptide_output`` or ``protein_output`` parameter if one or both is specified.

        This is convenient for setting a single ``backend`` or :py:func:`filtering options <scry.output.util.filter_psms>`.

    peptide_output : str | dict, optional
        If a string, the location for peptide output, in which case the backend and parameters from ``output`` will
        be used. If a :py:class:`dict`, parameters for output peptide-level results.

        Special keys:

        * ``backend``: The backend that will write peptide-level results.

          Either a string referring to a backend or plugin from :py:mod:`scry.output` registry, or a ``Callable``. If a
          callable the :py:class:`~wheely.mammoth.PsmDataset` will be passed as the first argument,
          and any other items in the dict will be passed as keyword arguments.

          Default: :py:func:`~scry.output.basic.write_parquet`

        * ``location``: if only this parameter is included, the backend and parameters from ``output`` will be used,
          with the specified location.

    protein_output : str | dict, optional
        If a string, the location for protein output, in which case the backend and parameters from ``output`` will
        be used. If a dict, parameters for output protein-level results.

        Special keys:

        * ``backend``: The backend that will write protein-level results
          Either a string referring to a backend or plugin from the :py:mod:`scry.output` registry, or a ``Callable``.
          If a callable the :py:class:`~wheely.mammoth.proteins.ProteinDataset` will be passed as
          the first argument, and any other items in the dict will be passed as keyword arguments.

          Default: :py:func:`~scry.output.basic.write_parquet`

        * ``location``: if only this parameter is included, the backend and parameters from ``output`` will be used,
          with the specified location.

    spark : SparkSession, optional
        A Spark session to use when creating the search result dataset. If ``None`` a session
        will be created with default configuration.

    Returns
    -------
    (peptide_result, protein_result) : tuple
        The returned value from the peptide and protein output backends. If either is ``None``,
        the corresponding result will be a `wheely-mammoth <https://github.com/seerbio/wheely-mammoth>`_ PSM/Protein
        dataset with intensity and confidence information.
    """
    search = search or dict()
    search_backend = search.pop("backend", "read_existing")
    if not callable(search_backend):
        search_backend = _get_search_backend(search_backend)

    search_start = _time()

    psms: _PsmDataset = search_backend(**search, spark=spark)

    search_end = _time()

    if not psms:
        raise ValueError("Got invalid PSM dataset from search backend!")

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Search stage found %d PSMs in %.02f sec",
            psms.data.count(),
            search_end - search_start,
        )

    model_start = _time()

    model: _BrewResult = _brew(psms, **(airpot or dict()))

    model_end = _time()

    _logger.info("Built rescoring model in %.02f sec", model_end - model_start)

    rescored: _PsmDataset
    try:
        rescored = model.psms
    except (AttributeError, KeyError) as e:
        raise TypeError(
            "Expected a RescoringResult, got " + type(model)
        ) from e

    assert (
        rescored is not None and rescored.data.count() > 0
    ), "Did not get any rescored PSMs!"

    # Allow mutation
    cortado = cortado.copy() if cortado else dict()

    # If unspecified, assume the first score is the rescored one
    score_name = cortado.pop(
        "score_column", next(iter(rescored.score_columns))
    )
    desc = cortado.pop("desc", False)

    _logger.info(
        'Assigning confidence across the dataset using "%s" (%sscending)',
        score_name,
        "a" if not desc else "de",
    )

    conf_start = _time()

    conf = _assign_confidence(
        rescored,
        score_column=score_name,
        desc=desc,
        **cortado,
    )

    conf_end = _time()

    n_confs = conf.data.count()
    _logger.info(
        "Assigned confidence to %d PSMs or peptides in %.02f sec",
        n_confs,
        conf_end - conf_start,
    )

    test_fdr = 0.01  # TODO
    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Found %d PSMs or peptides at %.0f%% FDR",
            conf.data.filter(conf.qvalues <= test_fdr).count(),
            100 * test_fdr,
        )

    # Get a table of peptides with their group and proteotypicity
    inf_start = _time()
    inference = _infer_spark(conf, **(proffer or dict()))
    inf_end = _time()

    # The result from Proffer will have list-valued protein groups; we need to join back together IDs
    # into a single string to get them into Spark via Pandas; ideally this could be avoided by using
    # Arrow instead as the interchange between Polars and Spark.
    protein_delim = conf.protein_delim or ";"
    inference_spark = conf.data.sparkSession.createDataFrame(
        inference.select(
            _pl.col("peptide").alias(conf.peptide_column),
            *[
                _pl.col(c).list.unique().list.sort().list.join(protein_delim)
                for c in ["protein_group", "all_proteins"]
                if c in inference.columns
            ],
            "proteotypic",
        ).to_pandas()
    )

    assert "protein_group" in inference_spark.columns

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Inferred %d protein groups in %.02f sec",
            inference_spark.select(_fns.countDistinct("protein_group"))
            .toPandas()
            .iloc[0, 0],
            inf_end - inf_start,
        )

    res_inferred = conf.with_data(
        conf.data.join(
            inference_spark,
            on=conf.peptide_column,
            how="leftouter",
        ),
        protein_column="protein_group",
        protein_delim=protein_delim,
        semantics={
            "proteotypic": _PROTEOTYPIC_PEPTIDE,
        },
    )

    # Compute protein FDR by rescoring protein groups
    # TODO: pluggable backends
    prot_conf_start = _time()
    prot_conf: _ProteinConfidenceDataset = _score_proteins(
        res_inferred,
        **(protein_scoring or {}),
    )
    prot_conf_end = _time()

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Scored %d protein groups in %.02f sec",
            prot_conf.data.count(),
            prot_conf_end - prot_conf_start,
        )

    if (
        c := getattr(prot_conf, "qvalue_column", None)
    ) is not None and _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Found %d protein groups at %.0f%% FDR",
            prot_conf.data.filter(_fns.col(c) <= test_fdr).count(),
            100 * test_fdr,
        )

    # Quantify peptides after they're annotated with groups, to simplify rollup
    peptide_quant = peptide_quant.copy() if peptide_quant else dict()
    quant_backend = peptide_quant.pop("backend", "basic")
    if not callable(quant_backend):
        quant_backend = _get_peptide_quant_backend(quant_backend)

    quant_start = _time()
    pep_quant_dset = quant_backend(
        res_inferred,
        **peptide_quant,
    )
    quant_end = _time()

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Quantified %d peptides in %.02f sec",
            pep_quant_dset.data.count(),
            quant_end - quant_start,
        )

    protein_rollup = protein_rollup.copy() if protein_rollup else dict()
    rollup_backend = protein_rollup.pop("backend", "basic")
    if not callable(rollup_backend):
        rollup_backend = _get_protein_rollup_backend(rollup_backend)

    rollup_start = _time()
    prot_quant_dset = rollup_backend(
        pep_quant_dset,
        **protein_rollup,
    )
    rollup_end = _time()

    _logger.info(
        "Rolled up protein group intensities in %.02f sec",
        rollup_end - rollup_start,
    )

    # Merge global protein confidence with per-sample quantifications
    protein_result = _merge_protein_confidence_and_quant(
        prot_conf, prot_quant_dset
    )

    # Get output backend
    output_backend = (output or {}).pop("backend", "write_parquet")
    if not callable(output_backend):
        output_backend = _get_output_backend(output_backend)

    # Write output
    output_res = output_backend(
        pep_quant_dset,
        protein_result,
        peptide_kwargs=peptide_output,
        protein_kwargs=protein_output,
        **output,
    )

    return output_res
