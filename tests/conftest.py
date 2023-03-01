"""
`conftest.py`: common configuration and fixtures for all unit tests
"""
import pytest

toml = pytest.importorskip("toml")

s3 = pytest.importorskip("s3fs")