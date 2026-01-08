"""
`scry.output`: package for "output" stage of the pipeline
"""

from .basic import write_csv, write_parquet
from .combined import write_combined
from .library import write_library
from .registry import register_backend, get_backend
