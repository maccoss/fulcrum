"""
`scry.scry`: this module contains the main entry point for Scry workflows
"""

from .workflow.v0 import scry_v0

_workflows = {
    "v0": scry_v0,
}


def scry(
    workflow: str = "v0",
    **kwargs,
):
    """
    `scry()`: run a Scry workflow using the specified parameters

    Parameters
    ----------
    workflow: str, optional
        The name of a packaged workflow. Default: "v0"
    **kwargs
        Any keyword arguments are passed directly to the workflow
    """

    _workflows[workflow](**kwargs)
