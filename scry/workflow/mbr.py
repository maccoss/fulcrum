"""
`scry.workflows.mbr`: ID and quantification workflow with 2-pass searching to provide "match-between-runs" (MBR)
"""

import copy as _copy
import logging as _logging
from time import time as _time
from typing import (
    Any as _Any,
    Callable as _Callable,
    Dict as _Dict,
    Optional as _Optional,
    Tuple as _Tuple,
    Union as _Union,
)

import json as _json
import toml as _toml
import polars as _pl
from pyspark.sql import (
    SparkSession as _SparkSession,
    functions as _fns,
)

from airpot import (
    brew as _brew,
    BrewResult as _BrewResult,
    RescoringResult as _RescoringResult,
)
from cortado import assign_confidence as _assign_confidence
from cortado.protein import score_proteins as _score_proteins
from proffer.spark import infer_spark as _infer_spark
from wheely.mammoth import (
    PsmDataset as _PsmDataset,
    ConfidenceDataset as _ConfidenceDataset,
)
from wheely.mammoth.proteins import (
    ProteinDataset as _ProteinDataset,
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


def mbr_workflow(
    library: _Optional[_Dict[str, _Any]] = None,
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
    Workflow for peptide and protein ID/quant with 2-pass searching to provide "match-between-runs" (MBR).

    Most steps of the workflow can use a variety of implementations, and support
    plugins to permit maximum flexibility.

    To use, call ``scry.scry(workflow="mbr")`` or run ``scry`` using the following TOML:

    .. code:: toml

        workflow = "mbr"

    Parameters
    ----------
    lib_params : dict, optional
        Parameters creating the first-pass library. If ``library.workflow`` is unspecified the ``v0`` workflow
        will be used.

        Currently only basic defaults are provided, meaning many parameters must be specified within ``library``
        despite corresponding parameters already being specified in this workflow's arguments. This is a known
        shortcoming for many common use cases and will likely be addressed in a future release.

        It is assumed that the workflow will return a :py:class:`~wheely.mammoth.ConfidenceDataset`
        of precursors when called without specifying a value for the ``output`` argument.
        Note that ``library.output`` will not be passed to ``library.workflow``, but instead will be used
        to separately create a library and pass it to the main search step. Depending on the value
        of ``search.use_library_location`` either ``library.output.location`` or the result of calling
        ``library.output.backend`` will be passed to the main search step as the ``library`` argument.
    search : dict, optional
        Specifies the backend and arguments that will give intial putative PSMs.

        Include a ``backend`` from the :py:mod:`scry.search` registry (default: :py:func:`~scry.search.read_existing`)
        or a suitable ``callable``.

        Most backends will accept a ``location`` as a string or list of strings giving the
        location(s) of MS data files.

        The backend must also accept a ``library`` argument which will be populated with the results of the first pass;
        any specified value for ``library`` will be ignored.

        For flexibility in the MBR process, the special key ``use_library_location``, if provided, will not be passed
        to the search backend. If the value is truthy, the location of the first-pass library will be passed to the
        search backend's ``library`` argument.
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
    # Set up defaults
    lib_params = _copy.deepcopy(library)
    if "workflow" not in lib_params:
        lib_params["workflow"] = "v0"
    if "search" not in lib_params:
        _search = _copy.deepcopy(search)
        _search.pop("use_library_location", None)
        lib_params["search"] = _search
    if (
        "cortado" not in lib_params
        or "pep_fdr_type" not in lib_params["cortado"]
    ):
        lib_params.setdefault("cortado", dict())[
            "pep_fdr_type"
        ] = "precursor-only"
    if "output" not in lib_params:
        lib_params["output"] = dict(
            backend="write_lib_params",
        )
    if "qval_thresh" not in lib_params["output"]:
        lib_params["output"]["qval_thresh"] = 0.01
    if "include_decoys" not in lib_params["output"]:
        lib_params["output"]["include_decoys"] = False
    elif lib_params["output"]["include_decoys"]:
        _logger.warning(
            "Library output will include decoys; this is not recommended!"
        )

    _logger.debug(
        "Computed first-pass parameters: %s", _json.dumps(lib_params)
    )

    lib_workflow = lib_params.pop("workflow")  # default configured above
    if lib_workflow != "v0":
        _logger.warning(
            "Got unexpected library workflow %s; proceed with caution!!",
            lib_workflow,
        )
    if not callable(lib_workflow):
        # Must avoid circular import
        from ..workflow import get_workflow as _get_workflow

        lib_workflow = _get_workflow(lib_workflow)

    if (
        lib_pep_fdr_type := lib_params.get("cortado", dict()).get(
            "pep_fdr_type", None
        )
    ) not in {"precursor-only", "peptide-only"}:
        _logger.warning(
            "Got unexpected library pep_fdr_type %s; proceed with caution!!!",
            lib_pep_fdr_type,
        )

    # Don't run the output stage for the library; we need access to the pre-output confidence results
    lib_output = lib_params.pop("output", dict()).copy()

    lib_start = _time()

    firstpass_prec_confs = lib_workflow(spark=spark, **lib_params)

    lib_end = _time()
    _logger.info(
        "Ran first-pass search and scoring in %.02f sec", lib_end - lib_start
    )

    lib_output_backend = lib_output.pop("backend")  # default configured above
    if not callable(lib_output_backend):
        lib_output_backend = _get_output_backend(lib_output_backend)

    lib_write_start = _time()
    lib_res = lib_output_backend(firstpass_prec_confs, **lib_output)
    lib_write_end = _time()
    _logger.info("Wrote library in %.02f sec", lib_write_end - lib_write_start)

    search = _copy.deepcopy(search or dict())

    # remove this parameter if it exists; we'll use the library from the first pass
    search.pop("library", None)

    if search.pop("use_library_location", False):
        lib = lib_output.get("location", None)
        _logger.info("Using library location %s for search", lib)
    else:
        lib = lib_res
        _logger.info("Using library result for search")

    search_backend = search.pop("backend", "read_existing")
    if not callable(search_backend):
        search_backend = _get_search_backend(search_backend)

    search_start = _time()

    psms: _PsmDataset = search_backend(**search, library=lib, spark=spark)

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

    if (pep_fdr_type := cortado.get("pep_fdr_type", None)) != "psm-only":
        _logger.warning(
            "Got unexpected pep_fdr_type %s; proceed with caution!!!",
            pep_fdr_type,
        )

    base_conf = _assign_confidence(
        rescored,
        score_column=score_name,
        desc=desc,
        **cortado,
    )
    conf = base_conf.with_data(
        base_conf.data.join(
            firstpass_prec_confs.data.select(
                firstpass_prec_confs.peptides.alias(base_conf.peptide_column),
                firstpass_prec_confs.charges.alias(base_conf.charge_column),
                firstpass_prec_confs.qvalues.alias("library-qvalue"),
            ),
            on=[base_conf.peptide_column, base_conf.charge_column],
            how="left",
        ).withColumns(
            {
                "combined-qvalue": _fns.greatest(
                    base_conf.qvalue_column,
                    _fns.col("library-qvalue"),
                )
            }
        ),
        qvalue_column="combined-qvalue",
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
    inference = _infer_spark(firstpass_prec_confs, **(proffer or dict()))
    inf_end = _time()

    # The result from Proffer will have list-valued protein groups; we need to join back together IDs
    # into a single string to get them into Spark via Pandas; ideally this could be avoided by using
    # Arrow instead as the interchange between Polars and Spark.
    protein_delim = conf.protein_delim or ";"
    inference_spark = conf.data.sparkSession.createDataFrame(
        inference.select(
            _pl.col("peptide").alias(conf.peptide_column),
            _pl.col("protein_group")
            .list.unique()
            .list.sort()
            .list.join(protein_delim),
            "proteotypic",
        ).to_pandas()
    )

    if _logger.isEnabledFor(_logging.INFO):
        _logger.info(
            "Inferred %d protein groups in %.02f sec",
            inference_spark.select(_fns.countDistinct("protein_group"))
            .toPandas()
            .iloc[0, 0],
            inf_end - inf_start,
        )

    firstpass_confs_inferred = firstpass_prec_confs.with_data(
        firstpass_prec_confs.data.join(
            inference_spark,
            on=firstpass_prec_confs.peptide_column,
            how="leftouter",
        ),
        protein_column="protein_group",
        protein_delim=protein_delim,
    )
    confs_inferred = conf.with_data(
        conf.data.join(
            inference_spark,
            on=conf.peptide_column,
            how="leftouter",
        ),
        protein_column="protein_group",
        protein_delim=protein_delim,
    )

    # Compute protein FDR by rescoring protein groups
    # TODO: pluggable backends
    prot_conf_start = _time()
    prot_conf = _score_proteins(
        firstpass_confs_inferred,
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
        confs_inferred,
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

    protein_result = prot_conf.with_data(
        prot_conf.data.join(
            prot_quant_dset.data.select(
                (
                    # To match the handling of protein group IDs above, we must join group IDs into strings
                    _fns.array_join(
                        (
                            prot_quant_dset.proteins
                            if prot_quant_dset.protein_delim is None
                            else _fns.split(
                                prot_quant_dset.proteins,
                                prot_quant_dset.protein_delim,
                            )
                        ),
                        prot_conf.protein_delim,
                    )
                ).alias(prot_conf.protein_column),
                prot_quant_dset.samples,
                prot_quant_dset.intensities,
            ),
            on=prot_conf.protein_column,
            how="leftouter",
        )
    )

    # Output peptide results
    if isinstance(peptide_output, str):
        pep_out_loc = peptide_output
        peptide_output = dict(
            output or dict(),
            location=pep_out_loc,
        )
    elif peptide_output:
        if set(peptide_output.keys()) == set(["location"]):
            pep_out_loc = peptide_output["location"]
            peptide_output = dict(
                output or dict(),
                location=pep_out_loc,
            )
        else:
            peptide_output = peptide_output.copy()
    else:
        peptide_output = dict()

    peptide_out_backend = peptide_output.pop("backend", "write_parquet")
    if not callable(peptide_out_backend):
        peptide_out_backend = _get_output_backend(peptide_out_backend)

    pep_out_start = _time()
    pep_out_res = peptide_out_backend(pep_quant_dset, **peptide_output)
    pep_out_end = _time()

    _logger.info(
        "Wrote peptide results in %.02f sec",
        pep_out_end - pep_out_start,
    )

    pep_res = pep_out_res or pep_quant_dset

    # Output protein results
    if isinstance(protein_output, str):
        prot_out_loc = protein_output
        protein_output = dict(
            output or dict(),
            location=prot_out_loc,
        )
    elif protein_output:
        if set(protein_output.keys()) == set(["location"]):
            prot_out_loc = protein_output["location"]
            protein_output = dict(
                output or dict(),
                location=prot_out_loc,
            )
        else:
            protein_output = protein_output.copy()
    else:
        protein_output = dict()

    protein_out_backend = protein_output.pop("backend", "write_parquet")
    if not callable(protein_out_backend):
        protein_out_backend = _get_output_backend(protein_out_backend)

    prot_out_start = _time()
    prot_out_res = protein_out_backend(protein_result, **protein_output)
    prot_out_end = _time()

    _logger.info(
        "Wrote protein results in %.02f sec",
        prot_out_end - prot_out_start,
    )

    prot_res = prot_out_res or protein_result

    return pep_res, prot_res
