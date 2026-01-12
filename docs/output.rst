Output
======

Scry provides various output backends for writing peptide and protein identification and quantification results
in different formats. Output backends can be combined with any workflow to generate results suitable for downstream
analysis or use with other proteomics tools.

Built-in Output Backends
------------------------

There are currently four built-in output backends in Scry:

* :py:func:`write_csv <scry.output.write_csv>` -- Write results to CSV format with optional filtering by quality threshold
* :py:func:`write_parquet <scry.output.write_parquet>` -- Write results to Parquet format for efficient storage and querying
* :py:func:`combined <scry.output.write_combined>` -- Write joined peptide and protein results to a single Parquet table with standardized column names
* :py:func:`write_library <scry.output.write_library>` -- Write results formatted for use as a spectral library compatible with DIA-NN and EncyclopeDIA

Configuring Output
------------------

Output backends are specified via the ``output`` parameter in TOML/JSON configuration. For example:

.. code :: toml

    output = "write_csv"

For complete information on configuring Scry workflows and passing parameters to the ``scry`` command line tool or Python API,
see :doc:`workflows`.

Filtering
~~~~~~~~~

Built-in output backends all results filtering using *q*-value threshold or by a custom boolean column.
The ``qval_thresh`` parameter specifies the maximum acceptable *q*-value. The ``include_decoys`` parameter controls
whether decoy identifications are included in the output. For more sophisticated filtering, a ``threshold_col``
parameter can specify a boolean column to determine which rows are written.

Output Backend Details
----------------------

CSV and Parquet Backends
~~~~~~~~~~~~~~~~~~~~~~~~

The :py:func:`write_csv <scry.output.write_csv>` and :py:func:`write_parquet <scry.output.write_parquet>` backends
write peptide and protein results independently to separate locations.

These backends provide direct access to the full datastructures computed by the Scry workflow. This is recommended for
users integrating Scry into bioinformatic pipelines or performing custom analyses, as they expose all available
information, however care must be taken to handle these outputs when varying the workflow configuration or choice of
backend modules. Notably, column names are not standardized and may vary between different search engines or
quantification methods. Additionally, the peptide output contains PSM-level data, but does not reflect protein-level
scoring, statistics, or filtering.

CSV output:

.. code :: toml

    [output]
    backend = "write_csv"

    [peptide_output]
    location = "results/scry-peptides/"

    [protein_output]
    location = "results/scry-proteins/"

Parquet output:

.. code :: toml

    [output]
    backend = "write_parquet"

    [peptide_output]
    location = "results/scry-peptides/"

    [protein_output]
    location = "results/scry-proteins/"

Parquet format is generally preferred for large result sets due to its columnar storage and compression efficiency.

Combined Backend
~~~~~~~~~~~~~~~~

The :py:func:`combined <scry.output.write_combined>` backend writes both peptide and protein results to a Parquet
dataset. Results are written with standardized column names to give a format similar to DIA-NN's report output.
This backend includes identification scores at both the PSM and protein group levels, as well as quantification values.

.. code :: toml

    [output]
    backend = "combined"
    location = "scry-combined-results/"

Key output columns include identification metrics (*q*-values, error probabilities), spectral properties
(retention time, precursor m/z, charge), and quantification results (raw and normalized intensities).

The combined format provides a single tabular format suitable for downstream statistical analysis or data visualization,
but requires users to carefully handle protein-level results, due to the duplication of protein group-level
information across multiple PSM rows. For protein-level analyses, it is recommended to instead use the ``write_parquet``
backend to provide direct access to lower-level data structures. However, users with existing analysis pipelines that
handle DIA-NN report formats may find the combined backend more convenient, with only minimal changes needed to support
Scry outputs.


Library Backend
~~~~~~~~~~~~~~~

The :py:func:`write_library <scry.output.write_library>` backend converts PSM identifications into a spectral
library format compatible with DIA-NN and EncyclopeDIA. Libraries are written in TSV format where each row
represents a single fragment ion. The library includes retention time, theoretical precursor properties, and
experimental fragment ion m/z and intensity values.

.. code :: toml

    [output]
    backend = "write_library"
    location = "results/library.tsv"
    spectra_backend = "mzml"

Spectral information for library generation is retrieved via pluggable backend implementations from ``wheely-mammoth``,
allowing flexibility in handling spectra from different data formats. The backend supports peptide sequence normalization
to ensure consistency with downstream tools. Quality filtering by *q*-value is applied to ensure high-confidence
identifications are included in the library.

Extending Output Backends
--------------------------

Custom output backends can be provided via the ``scry.output.plugins`` entry point group,
or registered in Python code using :py:func:`scry.output.register_backend`.

