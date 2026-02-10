"""These tests verify that the CLI works as expected."""

import subprocess

import pytest


def test_cli_help():
    """Test that the basic cli works with the help flag"""
    cmd = ["fulcrum", "--help"]
    subprocess.run(cmd, check=True)

    # TODO: assertions


@pytest.mark.parametrize(
    ["opt", "val_fixture"],
    [("--param-json", "param_json"), ("--json-file", "param_json_file")],
)
def test_cli_json(request, opt, val_fixture):
    """Test that the basic cli works with TOML inputs."""
    val = request.getfixturevalue(val_fixture)

    cmd = ["fulcrum", opt, val, "-v"]
    subprocess.run(cmd, check=True)

    # TODO: assertions


@pytest.mark.parametrize(
    ["opt", "val_fixture"],
    [("--param-toml", "param_toml"), ("--toml-file", "param_toml_file")],
)
def test_cli_toml(request, opt, val_fixture):
    """Test that the basic cli works with TOML inputs."""
    pytest.importorskip("toml")  # skip this test if `toml` isn't installed

    val = request.getfixturevalue(val_fixture)

    cmd = ["fulcrum", opt, val, "-v"]
    subprocess.run(cmd, check=True)

    # TODO: assertions


def test_cli_none():
    """Test that the basic cli without arguments crashes."""
    cmd = ["fulcrum"]

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(cmd, check=True)


_args = [
    "--param-json",
    "--json-file",
    "--param-toml",
    "--toml-file",
]


@pytest.mark.parametrize(
    ["arg1", "arg2"],
    [
        (a, b)
        for i, a in enumerate(_args)
        for j, b in enumerate(_args)
        if i < j
    ],
)
def test_cli_multi(arg1, arg2):
    """Test that the basic cli with too many arguments crashes."""
    cmd = ["fulcrum", arg1, "foo", arg2, "bar"]

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(cmd, check=True)
