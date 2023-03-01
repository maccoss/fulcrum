"""
`scry.py`: CLI for Scry
"""

import json
import logging
from urllib.parse import urlparse

import click
import fsspec


_logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--param-json',
    required=False,
    help="A JSON-encoded parameter string"
)
@click.option(
    "--json-file",
    required=False,
    help="A URI or local path to a JSON-encoded parameter file",
)
@click.option(
    '--param-toml',
    required=False,
    help="A TOML-encoded parameter string"
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
    help="Increase the level of verbosity. May be specified multiple times."
)
@click.option(
    "-q",
    "--quiet",
    count=True,
    help="Decrease the level of verbosity. May be specified multiple times."
)
def main(verbose, quiet, param_json=None, json_file=None, param_toml=None, toml_file=None):
    """
    Entry point for Scry CLI
    """

    set_log_level(verbose, quiet)

    params_dict = _parse_args(param_json, json_file, param_toml, toml_file)

    _logger.debug("Parsed parameters: %s", json.dumps(params_dict))

    raise NotImplementedError("TODO")  # TODO


def set_log_level(verbose: int, quiet: int):
    """
    Set up the logger based on the specified flags.
    """

    if verbose and quiet:
        raise ValueError("The --verbose and --quiet flags are mutually exclusive!")

    _def = 2  # WARNING
    _lvls = [
        logging.CRITICAL,
        logging.ERROR,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
    ]

    logging.basicConfig(level=_lvls[_def - quiet + verbose])


def _parse_args(param_json=None, json_file=None, param_toml=None, toml_file=None):
    """
    Parse a parameters dictionary from **exactly one** of the provided formats.
    """

    if sum(int(v is not None) for v in [param_json, json_file, param_toml, toml_file]) != 1:
        raise ValueError("You must specify exactly one of {--param-json, --json-file, --param-toml, --toml-file}!")

    if param_json is not None:
        return json.loads(param_json)
    if json_file is not None:
        with fsspec.filesystem(urlparse(json_file, "file").scheme).open(json_file, "r") as f:
            return json.load(f)

    import toml
    if param_toml is not None:
        return toml.loads(param_toml)
    if toml_file is not None:
        with fsspec.filesystem(urlparse(toml_file, "file").scheme).open(toml_file, "r") as f:
            return toml.load(f)