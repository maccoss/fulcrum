"""
`conftest.py`: common configuration and fixtures for all unit tests
"""

import json

import pytest


@pytest.fixture
def param_dict():
    """
    Mock parameters in a dict
    """
    return {}


@pytest.fixture
def param_json(param_dict):
    return json.dumps(param_dict)


@pytest.fixture
def param_json_file(param_dict, tmp_path):
    path = tmp_path / "mock_params.json"
    with open(path, "w") as f:
        json.dump(param_dict, f)

    return path


@pytest.fixture
def param_toml(param_dict):
    toml = pytest.importorskip("toml")

    return toml.dumps(param_dict)


@pytest.fixture
def param_toml_file(param_dict, tmp_path):
    toml = pytest.importorskip("toml")

    path = tmp_path / "mock_params.toml"
    with open(path, "w") as f:
        toml.dump(param_dict, f)

    return path
