Quickstart guide
=====================

Installation
------------

This library requires Python 3.8+ and can be installed with pip:

.. code:: shell

   pip install scry-ms

You may also need to install Java if you intend to run Scry workflows
locally.

Using Scry on Databricks
------------------------

Scry is built to quickly run in a Databricks notebook environment. After
setting up a cluster, you can install directly from your notebook:

::

   %pip install scry-ms

When invoking Scry you should specify the ``SparkSession`` in use using
the ``spark`` keyword parameter:

.. code:: python

   from scry import scry

   scry(spark=spark, **params)

For more about running Scry in Databricks, see `Python Usage`_.

CLI Usage
---------

Scry includes a CLI that permits running a workflow using TOML parameters:

.. code:: shell

   scry -v --param-toml '
   workflow = "v0"

   [search]
   backend = "read_existing"
   engine = "encyclopedia"
   location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
   '

The CLI will accept JSON or TOML as either a string or a file:

.. code:: shell

   # JSON string
   scry --param-json '{
       "workflow": "v0",
       "search": {
         "backend": "read_existing",
         "engine": "encyclopedia",
         "location": "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
       }
   }'

   # JSON file
   scry --json-file path/to/file.json

   # TOML file
   scry --toml-file path/to/file.toml

Python Usage
------------

The full flexibility of Scry is available through the Python library's :py:func:`~scry.scry.scry`.
Usage is similar from a REPL or notebook interface:

.. code:: pycon

   >>> import logging; logging.getLogger().setLevel("INFO")
         >>> from fulcrum import fulcrum
         >>> scry(
         ...   workflow = "v0",
         ...   search = dict(
         ...     backend = "read_existing",
         ...     engine = "encyclopedia",
         ...     location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
         ...   )
         ... )
         INFO:scry.workflow.v0:Search stage found 1770 PSMs in 4.24 sec
         INFO:scry.workflow.v0:Built rescoring model in 3.57 sec
         INFO:scry.workflow.v0:Assigning confidence across the dataset using "mokapot score" (ascending)
         INFO:scry.workflow.v0:Assigned confidence to 832 PSMs or peptides in 2.81 sec
         INFO:scry.workflow.v0:Found 522 PSMs or peptides at 1% FDR

      For full documentation, see :py
      >>> from scry import scry
      >>> scry(
      ...   workflow = "v0",
      ...   search = dict(
      ...     backend = "read_existing",
      ...     engine = "encyclopedia",
      ...     location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
      ...   )
      ... )
      INFO:scry.workflow.v0:Search stage found 1770 PSMs in 4.24 sec
      INFO:scry.workflow.v0:Built rescoring model in 3.57 sec
      INFO:scry.workflow.v0:Assigning confidence across the dataset using "mokapot score" (ascending)
      INFO:scry.workflow.v0:Assigned confidence to 832 PSMs or peptides in 2.81 sec
      INFO:scry.workflow.v0:Found 522 PSMs or peptides at 1% FDR

   For full documentation, see :py
      >>> from fulcrum import scry
      >>> scry(
      ...   workflow = "v0",
      ...   search = dict(
      ...     backend = "read_existing",
      ...     engine = "encyclopedia",
      ...     location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
      ...   )
      ... )
      INFO:scry.workflow.v0:Search stage found 1770 PSMs in 4.24 sec
      INFO:scry.workflow.v0:Built rescoring model in 3.57 sec
      INFO:scry.workflow.v0:Assigning confidence across the dataset using "mokapot score" (ascending)
      INFO:scry.workflow.v0:Assigned confidence to 832 PSMs or peptides in 2.81 sec
      INFO:scry.workflow.v0:Found 522 PSMs or peptides at 1% FDR

   For full documentation, see :py
   >>> from scry import scry
   >>> scry(
   ...   workflow = "v0",
   ...   search = dict(
   ...     backend = "read_existing",
   ...     engine = "encyclopedia",
   ...     location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
   ...   )
   ... )
   INFO:scry.workflow.v0:Search stage found 1770 PSMs in 4.24 sec
   INFO:scry.workflow.v0:Built rescoring model in 3.57 sec
   INFO:scry.workflow.v0:Assigning confidence across the dataset using "mokapot score" (ascending)
   INFO:scry.workflow.v0:Assigned confidence to 832 PSMs or peptides in 2.81 sec
   INFO:scry.workflow.v0:Found 522 PSMs or peptides at 1% FDR

For full documentation, see :py:func:`API Reference <scry.scry.scry>`.

Configuring Spark
-----------------
You may configure a connection to a Spark cluster by providing an appropriate
``spark_config`` section in the workflow parameters:

.. code:: toml

    [spark_config]
    "spark.master"="local[*]"
    "driver.memory"="4g"

When calling Scry from Python, you can either specify a ``spark_config`` or
pass a :py:class:`SparkSession` using the ``spark`` parameter.

.. code:: python

    scry(
        spark=spark_session,
    )

    # OR

    scry(
        spark_config={
            "spark.master": "local[*]",
            "driver.memory": "4g",
        },
    )
