Workflows
=========

Fulcrum consists of various modules that can be used in various proteomics
analysis workflows. Some workflows are built into Fulcrum, and it's possible
to develop new workflows as plugins (for more, see :py:class:`fulcrum.workflow`).

Built-in Workflows
------------------

There are currently three built-in workflows in Fulcrum:

* :py:func:`v0 <fulcrum.workflow.v0.v0_workflow>` -- Workflow for identifying precursors and estimating confidence, capable of
  basic analyses or library building
* :py:func:`v1 <fulcrum.workflow.v1.v1_workflow>` -- Workflow for peptide and protein ID and quant
* :py:func:`mbr <fulcrum.workflow.mbr.mbr_workflow>` -- Workflow for two-pass searching, first building a library then
  searching with it, including peptide and protein ID and quant

Configuring Workflows
---------------------

Fulcrum workflows are configured via TOML/JSON parameters. For example:

.. code :: toml

    workflow = "v0"

    [search]
    backend = "read_existing"
    engine = "encyclopedia"
    location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"

You can pass TOML to the ``fulcrum`` command line tool in a file:

.. code :: shell

    fulcrum --toml-file path/to/fulcrum-params.toml

or as a string:

.. code :: shell

    fulcrum --param-toml '
    workflow = "v0"

    [search]
    backend = "read_existing"
    engine = "encyclopedia"
    location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
    '

The same set of parameters are available as keyword arguments when calling :py:func:`~fulcrum.fulcrum.fulcrum`
from Python:

.. code :: python

    fulcrum.fulcrum(
        workflow="v0",
        search=dict(
            backend="read_existing",
            engine="encyclopedia",
            location="data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
        ),
    )

or with a :py:class:`dict` of parameters:

.. code :: python

    fulcrum.fulcrum(**params_dict)
