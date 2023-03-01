"""These tests verify that the Scry CLI works as expected."""

import subprocess

import pytest


def test_cli_basic():
    """Test that the basic cli works."""
    cmd = ["scry"]  # TODO: will crash
    subprocess.run(cmd, check=True)

    # TODO: assertions


def test_cli_none():
    """Test that the basic cli without arguments crashes."""
    cmd = ["scry"]

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
    ]
)
def test_cli_multi(arg1, arg2):
    """Test that the basic cli with too many arguments crashes."""
    cmd = ["scry", arg1, "foo", arg2, "bar"]

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(cmd, check=True)
