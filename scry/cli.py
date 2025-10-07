"""
`scry.py`: CLI for Scry
"""

import json
import logging
from urllib.parse import urlparse

import click
import fsspec

from .scry import scry


_logger = logging.getLogger(__name__)


@click.command(
    help="""
    CLI for the Scry search pipeline. Parameters should be specified using
    exactly one of the TOML/JSON options listed below.

    For information on allowed parameters within the TOML/JSON, run the
    following command:

    python -c 'import scry; help(scry.scry)'
    """
)
@click.option(
    "--param-json", required=False, help="A JSON-encoded parameter string"
)
@click.option(
    "--json-file",
    required=False,
    help="A URI or local path to a JSON-encoded parameter file",
)
@click.option(
    "--param-toml", required=False, help="A TOML-encoded parameter string"
)
@click.option(
    "--toml-file",
    required=False,
    help="A URI or local path to a TOML-encoded parameter file",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase the level of verbosity. May be specified multiple times.",
)
@click.option(
    "-q",
    "--quiet",
    count=True,
    help="Decrease the level of verbosity. May be specified multiple times.",
)
def main(
    verbose,
    quiet,
    param_json=None,
    json_file=None,
    param_toml=None,
    toml_file=None,
):
    """
    CLI for the Scry search pipeline. Parameters should be specified using
    exactly one of the TOML/JSON options listed below.

    For information on allowed parameters within the TOML/JSON, see
    :py:func:`scry.scry`, or run the following command:

    .. code:: shell

        python -c 'import scry; help(scry.scry)'

    """

    set_log_level(verbose, quiet)

    params_dict = _parse_args(param_json, json_file, param_toml, toml_file)

    _logger.debug("Parsed parameters: %s", json.dumps(params_dict))

    scry(**params_dict)


def set_log_level(verbose: int, quiet: int):
    """
    Set up the logger based on the specified flags.
    """

    if verbose and quiet:
        raise ValueError(
            "The --verbose and --quiet flags are mutually exclusive!"
        )

    _def = 2  # WARNING
    _lvls = [
        logging.CRITICAL,
        logging.ERROR,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
    ]

    def clamp(n, start, end):
        return max(start, min(n, end))

    def get_level(n):
        return _lvls[clamp(n, 0, len(_lvls) - 1)]

    level = _def - quiet + verbose

    logging.basicConfig(level=get_level(level), force=True)

    # Now override some particularly verbose loggers at certain levels.
    # This has the effect of essentially adding a level beyond DEBUG (accessible
    # by adding more -v flags) at which these highly-verbose logs are enabled.

    if level >= _lvls.index(logging.INFO):
        # py4j is somewhat verbose at info level (and more so at debug); force it an extra step quieter.
        # Its INFO level will be available at -vvv (beyond DEBUG) and its DEBUG level at -vvvv (further beyond DEBUG)
        logging.getLogger("py4j").setLevel(get_level(level - 2))

        # directlfq is very verbose at info level; force it two steps quieter.
        # Its INFO level will be available at -vvv (beyond DEBUG)
        logging.getLogger("directlfq").setLevel(get_level(level - 2))

    if level >= _lvls.index(logging.DEBUG):
        # numba is very verbose at debug level; force an extra step quieter.
        # Its DEBUG level will be available at -vvv (beyond DEBUG)
        logging.getLogger("numba").setLevel(get_level(level - 1))

        # proffer is very verbose at debug level; force an extra step quieter.
        # Its DEBUG level will be available at -vvv (beyond DEBUG)
        logging.getLogger("proffer").setLevel(get_level(level - 1))


def _parse_args(
    param_json=None, json_file=None, param_toml=None, toml_file=None
):
    """
    Parse a parameters dictionary from **exactly one** of the provided formats.
    """

    if (
        sum(
            int(v is not None)
            for v in [param_json, json_file, param_toml, toml_file]
        )
        != 1
    ):
        raise ValueError(
            "You must specify exactly one of {--param-json, --json-file, --param-toml, --toml-file}!"
        )

    if param_json is not None:
        return json.loads(param_json)
    if json_file is not None:
        with fsspec.filesystem(urlparse(json_file, "file").scheme).open(
            json_file, "r"
        ) as f:
            return json.load(f)

    import toml

    if param_toml is not None:
        return toml.loads(param_toml)
    if toml_file is not None:
        with fsspec.filesystem(urlparse(toml_file, "file").scheme).open(
            toml_file, "r"
        ) as f:
            return toml.load(f)
