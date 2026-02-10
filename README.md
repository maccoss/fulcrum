<img alt="scry logo" src="./docs/_static/scry-logo.png" height="128" align="left" style="margin: 8px">

**Fulcrum** is a search pipeline for extreme-scale proteomics
experiments. It's based on composable, modular implementations using Spark to
attain near-infinite scalability.

## Installation  

This library requires Python 3.8+ and can be installed with pip:  

```shell
pip install fulcrum-ms
```

You may also need to install Java if you intend to run Scry workflows locally.

## Using Scry on Databricks

Scry is built to quickly run in a Databricks notebook environment. After setting
up a cluster, you can install directly from your notebook:

```
%pip install scry-ms
```

When invoking Scry you should specify the `SparkSession` in use using the
`spark` keyword parameter:

```python
from fulcrum import fulcrum

fulcrum(spark=spark, **params)
```

## CLI Usage

Scry includes a CLI that permits running a workflow using TOML
parameters:

``` shell
scry -v --param-toml '
workflow = "v0"

[search]
backend = "read_existing"
engine = "encyclopedia"
location = "data/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
'
```

The CLI will accept JSON or TOML as either a string or a file:

``` shell
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
```

## Python Usage

The full flexibility of Scry is available through the Python library's
`scry` function. Usage is similar from a REPL or notebook interface:

``` pycon
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
```

## Configuring Spark

You may configure a connection to a Spark cluster by providing an
appropriate `spark_config` section in the workflow parameters:

``` toml
[spark_config]
"spark.master"="local[*]"
"driver.memory"="4g"
```

When calling Scry from Python, you can either specify a
[spark_config]{.title-ref} or pass a `SparkSession`{.interpreted-text
role="py:class"} using the [spark]{.title-ref} parameter.

``` python
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
```
