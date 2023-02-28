"""
`scry.py`: CLI for Scry
"""

import click


@click.command()
@click.option(
    '--param-json',
    required=False,
    help="A JSON-encoded parameter string"
)
@click.option(
    "--param-file",
    required=False,
    help="A URI or local path to a JSON-encoded parameter file",
)
def main(param_json=None, param_file=None):
    """
    Entry point for Scry CLI
    """
    raise NotImplementedError("TODO")
