Scry documentation
==================

.. image:: _static/scry-logo.png
   :height: 128px
   :alt: The logo of Scry, showing a crystal ball sitting on a marble column

**Scry** is a search pipeline for extreme-scale
proteomics experiments. It’s based on composable, modular
implementations using Spark to attain near-infinite scalability.

Getting Started
---------------

For help installing and running Scry, see the :doc:`quickstart`.

Scry Workflows
--------------

Scry consists of various modules that can be used in various proteomics
analysis workflows. Some workflows are built into Scry, and it's possible
to develop new workflows as plugins (for more, see :py:class:`scry.workflow`).

Configuring Workflows
---------------------

Scry workflows are configured via TOML/JSON parameters, or when invoked from
Python, a :py:class:`dict` with the same structure. For example:

.. code :: toml

    workflow = "v1"

    [search]
    backend = "read_existing"
    location = ["…"]

To learn more about the available workflows and how to configure them,
see :doc:`workflows`.

Contents
========

.. toctree::
    :maxdepth: 4

    quickstart
    workflows
