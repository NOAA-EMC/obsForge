import pytest
import os
import subprocess
from pathlib import Path
from datetime import datetime

@pytest.fixture
def script_env(tmp_path):
    # Create dummy HOMEobsforge/parm/config.yaml
#    parm_dir = tmp_path / "parm"
#    parm_dir.mkdir(parents=True)
#    config_path = parm_dir / "config.yaml"
#
#    config_path.write_text("""
#    obsforge:
#      some_yaml_key: some_yaml_value
#    marinedump:
#      dump_param: test_value
#    """)
#
    # Set environment vars expected by the script
    env = os.environ.copy()
    env["cyc"] = "0"
    env["current_cycle"] = "2021070100"
    env["PDY"] = "20210701"
    env["RUN"] = "gdas"
    return env

def test_run_exobsforge_script(script_env):

    env = script_env

    # Run the script using subprocess
    result = subprocess.run(
        ["python3", "../exobsforge_global_marine_dump.py"],
        cwd=Path(__file__).parent,
        env=env,
        capture_output=True,
        text=True
    )

    # Print the standard output
    print("Standard Output:")
    print(result.stdout)

    # Optionally, print the standard error
    print("Standard Error:")
    print(result.stderr)

    # Basic assertions
    assert result.returncode == 0
