"""These tests verify that the Scry CLI works as expected."""

import subprocess


def test_cli_basic():
    """Test that the basic cli works."""
    cmd = ["scry"]
    subprocess.run(cmd, check=True)

    # TODO: assertions
