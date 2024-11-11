"""
`scry.workflow`: package for various pre-built workflows
"""

from .v0 import scry_v0
from .v1 import scry_v1

from .registry import get_workflow, register_workflow
