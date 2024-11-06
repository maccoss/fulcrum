"""
`scry.workflow`: package for various pre-built workflows
"""

from .v0 import scry_v0

from .registry import get_workflow, register_workflow, _workflows

workflows = _workflows
