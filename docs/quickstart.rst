Quickstart guide
=====================

Installation
------------

This library requires Python 3.10+ and can be installed with pip:

.. code:: shell

   pip install fulcrum-ms

You may also need to install Java if you intend to run Fulcrum workflows
locally.

Using Fulcrum on Databricks
---------------------------

Fulcrum is built to quickly run in a Databricks notebook environment. After
setting up a cluster, you can install directly from your notebook:

::

   %pip install fulcrum-ms

When invoking Fulcrum you should specify the ``SparkSession`` in use using
the ``spark`` keyword parameter:

.. code:: python

   from fulcrum import fulcrum

   fulcrum(spark=spark, **params)

For more about running Fulcrum in Databricks, see `Python Usage`_.

CLI Usage
---------

Fulcrum includes a CLI that permits running a workflow using TOML parameters:

.. code:: shell

   fulcrum -v --param-toml '
   workflow = "v0"

   [search]
   backend = "read_existing"
   engine = "encyclopedia"
   location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
   '

The CLI will accept JSON or TOML as either a string or a file:

.. code:: shell

   # JSON string
   fulcrum --param-json '{
       "workflow": "v0",
       "search": {
         "backend": "read_existing",
         "engine": "encyclopedia",
         "location": "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
       }
   }'

   # JSON file
   fulcrum --json-file path/to/file.json

   # TOML file
   fulcrum --toml-file path/to/file.toml

Python Usage
------------

The full flexibility of Fulcrum is available through the Python library's :py:func:`~fulcrum.fulcrum.fulcrum`.
Usage is similar from a REPL or notebook interface:

.. code:: pycon

   >>> import logging; logging.getLogger().setLevel("INFO")
   >>> from fulcrum import fulcrum
   >>> fulcrum(
   ...   workflow = "v0",
   ...   search = dict(
   ...     backend = "read_existing",
   ...     engine = "encyclopedia",
   ...     location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt",
   ...   )
   ... )
   INFO:fulcrum.workflow.v0:Search stage found 1770 PSMs in 4.24 sec
   INFO:fulcrum.workflow.v0:Built rescoring model in 3.57 sec
   INFO:fulcrum.workflow.v0:Assigning confidence across the dataset using "mokapot score" (ascending)
   INFO:fulcrum.workflow.v0:Assigned confidence to 832 PSMs or peptides in 2.81 sec
   INFO:fulcrum.workflow.v0:Found 522 PSMs or peptides at 1% FDR

For full documentation, see :py:func:`API Reference <fulcrum.fulcrum.fulcrum>`.

Configuring Spark
-----------------
You may configure a connection to a Spark cluster by providing an appropriate
``spark_config`` section in the workflow parameters:

.. code:: toml

    [spark_config]
    "spark.master"="local[*]"
    "driver.memory"="4g"

When calling Fulcrum from Python, you can either specify a ``spark_config`` or
pass a :py:class:`SparkSession` using the ``spark`` parameter.

.. code:: python

    fulcrum(
        spark=spark_session,
    )

    # OR

    fulcrum(
        spark_config={
            "spark.master": "local[*]",
            "driver.memory": "4g",
        },
    )
