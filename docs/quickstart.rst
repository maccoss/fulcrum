Quickstart guide
=====================

Installation
------------

This library requires Python 3.8+ and can be installed with pip:

.. code:: shell

   pip install scry-ms

You may also need to install Java if you intend to run Scry workflows
locally.

Installing in Databricks
~~~~~~~~~~~~~~~~~~~~~~~~

Scry is built to quickly run in a Databricks notebook environment. After
setting up a cluster, you can install directly from your notebook:

::

   %pip install scry-ms

When invoking Scry you should specify the ``SparkSession`` in use using
the ``spark`` keyword parameter:

.. code:: python

   from scry import scry

   scry(spark=spark, **params)

Basic Usage
-----------

Python
~~~~~~

Scry performs most processing with the Spark runtime, so running
workflows locally will require a Java installation. You may also
configure a connection to a Spark cluster by providing an appropriate
``SparkSession`` instance when invoking the API.

The full flexibility of Scry is available through the Python library.
Usage is similar from a REPL or notebook interface:

.. code:: pycon

   >>> import logging; logging.getLogger().setLevel("INFO")
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

CLI
~~~

Scry includes a basic CLI that allows access to a subset of the library
functionality via JSON or TOML parameters:

.. code:: shell

   $ scry -v --param-toml '
   > workflow = "v0"
   >
   > [search]
   > backend = "read_existing"
   > engine = "encyclopedia"
   > location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
   > '
   INFO:scry.workflow.v0:Search stage found 1770 PSMs in 4.24 sec
   INFO:scry.workflow.v0:Built rescoring model in 3.57 sec
   INFO:scry.workflow.v0:Assigning confidence across the dataset using "mokapot score" (ascending)
   INFO:scry.workflow.v0:Assigned confidence to 832 PSMs or peptides in 2.81 sec
   INFO:scry.workflow.v0:Found 522 PSMs or peptides at 1% FDR

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
