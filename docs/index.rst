Fulcrum Pipeline documentation
==============================

.. image:: _static/scry-logo.png
   :height: 128px
   :alt: The logo of Scry, showing a crystal ball sitting on a marble column

**Fulcrum Pipeline**™ is a tool for extreme-scale
proteomics experiments. It's based on composable, modular
implementations using Spark to attain near-infinite scalability.

Getting Started
---------------

For help installing and running Fulcrum, see the :doc:`quickstart`.

Fulcrum Workflows
-----------------

Fulcrum consists of various modules that can be used in various proteomics
analysis workflows. Some workflows are built into Fulcrum, and it's possible
to develop new workflows as plugins (for more, see :py:class:`fulcrum.workflow`).

Configuring Workflows
---------------------

Fulcrum workflows are configured via TOML/JSON parameters, or when invoked from
Python, a :py:class:`dict` with the same structure. For example:

.. code :: toml

    workflow = "v0"

    [search]
    backend = "read_existing"
    engine = "encyclopedia"
    location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"

To learn more about the available workflows and how to configure them,
see :doc:`workflows`.

Contents
========

.. toctree::
    :maxdepth: 4

    quickstart
    workflows
    output
