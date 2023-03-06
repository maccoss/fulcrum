"""
`scry.output`: package for "output" stage of the pipeline
"""

from .confidence import write_csv

output_backends = {
    "write_csv": write_csv,
}
