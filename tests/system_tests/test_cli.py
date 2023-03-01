"""These tests verify that the Scry CLI works as expected."""

import subprocess

import pytest


@pytest.mark.parametrize(
    ["opt", "val_fixture"],
    [
        ("--param-json", "param_json"),
        ("--json-file", "param_json_file")
    ],
)
def test_cli_json(request, opt, val_fixture):
    """Test that the basic cli works with TOML inputs."""
    pytest.importorskip("toml")

    val = request.getfixturevalue(val_fixture)

    cmd = ["scry", opt, val]
    subprocess.run(cmd, check=True)


@pytest.mark.parametrize(
    ["opt", "val_fixture"],
    [
        ("--param-toml", "param_toml"),
        ("--toml-file", "param_toml_file")
    ],
)
def test_cli_toml(request, opt, val_fixture):
    """Test that the basic cli works with TOML inputs."""
    pytest.importorskip("toml")

    val = request.getfixturevalue(val_fixture)

    cmd = ["scry", opt, val]
    subprocess.run(cmd, check=True)


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
