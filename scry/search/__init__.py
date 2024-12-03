"""
`scry.search`: package for "search" stage of the pipeline
"""

from .existing import read_existing_results as read_existing
from .registry import register_backend, get_backend
