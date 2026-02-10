"""
`fulcrum.workflow`: package for various pre-built workflows

In addition to the built-in workflows provided in this module,
it's possible to use plugin workflows by passing their name to
:py:func:`get_workflow`. Plugins are automatically discovered
based on package metadata, or can be registered manually by
calling :py:func:`register_workflow`.
"""

from .v0 import v0_workflow
from .v1 import v1_workflow
from .mbr import mbr_workflow

from .registry import get_workflow, register_workflow
