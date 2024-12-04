Workflows
=========

Scry consists of various modules that can be used in various proteomics
analysis workflows. Some workflows are built into Scry, and it's possible
to develop new workflows as plugins (for more, see :py:class:`scry.workflow`).

Built-in Workflows
------------------

There are currently two built-in workflows in Scry:

* :py:func:`v0 <scry.workflow.v0.scry_v0>` -- Workflow for identifying precursors and estimating confidence
* :py:func:`v1 <scry.workflow.v1.scry_v1>` -- Workflow for peptide and protein ID and quant

Configuring Workflows
---------------------

Scry workflows are configured via TOML/JSON parameters. For example:

.. code :: toml

    workflow = "v0"

    [search]
    backend = "read_existing"
    engine = "encyclopedia"
    location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"

You can pass TOML to the ``scry`` command line tool in a file:

.. code :: shell

    scry --toml-file path/to/scry-params.toml

or as a string:

.. code :: shell

    scry --param-toml '
    workflow = "v0"

    [search]
    backend = "read_existing"
    engine = "encyclopedia"
    location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
    '

The same set of parameters are available as keyword arguments when calling :py:func:`~scry.scry.scry`
from Python:

.. code :: python

    scry.scry(
        workflow="v0",
        search=dict(
            backend="read_existing",
            engine="encyclopedia",
            location="data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
        ),
    )

or with a :py:class:`dict` of parameters:

.. code :: python

    scry.scry(**params_dict)
