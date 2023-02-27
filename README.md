<img alt="scry logo" src="./static/scry-logo.png" height="128" align="left" style="margin: 8px">

**Scry** is an in-development search pipeline for extreme-scale proteomics
experiments. It's based on composable, modular implementations using Spark to
attain near-infinite scalability.

## Installation  

This library requires Python 3.8+ and can be installed with pip:  

```shell
pip install scry-ms
```

You may also need to install Java if you intend to run Scry workflows locally.

### Installing in Databricks

Scry is built to quickly run in a Databricks notebook environment. After setting
up a cluster, you can install directly from your notebook:

```
%pip install scry-ms
```

When invoking Scry you should specify the `SparkSession` in use using the
`spark` keyword parameter.

## Basic Usage  

Scry performs most processing with the Spark runtime, so running workflows
locally will require a Java installation.  You may also configure a connection
to a Spark cluster by providing an appropriate `SparkSession` instance when
invoking the API.

**TODO:** include a brief demo of using the library from the Python REPL:

```pycon
>>> from my.favorite.library import function
>>> res = function(*args)  # TODO
>>> print(res)
Some amazing output goes here: Hello World.
```
