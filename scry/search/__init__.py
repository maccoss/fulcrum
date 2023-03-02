"""
`scry.search`: package for "search" stage of the pipeline
"""

from .existing import read_existing_results

search_backends = {
    "read_existing": read_existing_results,
}
