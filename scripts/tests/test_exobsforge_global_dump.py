import pytest
import os
import subprocess
from pathlib import Path


@pytest.fixture
def script_env(tmp_path):
    # Set environment vars expected by the script
    env = os.environ.copy()
    env["cyc"] = "0"
    env["current_cycle"] = "20250301600"
    env["PDY"] = "20250316"
    env["RUN"] = "gdas"
    return env


def create_dcom(output_root="./", dcom_tree_file="dcom_tree.txt"):
    # Create a directory structure based on a tree file
    with open(dcom_tree_file, "r") as f:
        lines = f.readlines()

    stack = []

    for line in lines:
        line = line.rstrip()

        # Skip root and empty lines
        if not line.strip() or line.startswith("/"):
            continue

        # Determine depth based on presence of box characters and spacing
        depth = line.count("│") + line.count("    ")  # '    ' = 4 spaces = 1 level

        # Extract the name after the ├── or └──
        if "──" in line:
            name = line.split("──", 1)[1].strip()
        else:
            continue  # skip anything without file/dir name

        # Adjust the stack based on depth
        stack = stack[:depth]
        stack.append(name)

        full_path = os.path.join(output_root, *stack)

        if name.endswith(".nc") or name.endswith(".h5"):
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(b"")  # empty file
        else:
            os.makedirs(full_path, exist_ok=True)


def test_run_exobsforge_script(script_env):

    env = script_env

    # Prepare a mocked dcom directory
    create_dcom(output_root=os.getenv("DCOMROOT"),
                dcom_tree_file=Path(__file__).parent / "dcom_tree.txt")

    # List of scripts to run
    scripts = [
        "exobsforge_global_marine_dump.py",
        "exobsforge_global_aod_dump.py"
    ]

    for script_name in scripts:
        # Run each script using subprocess
        exec = Path(__file__).parent.parent / script_name
        cwd = Path(__file__).parent / "tests_output/RUNDIRS/obsforge"
        result = subprocess.run(
            ["python3", exec],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True
        )

        print(f"Standard Output for {script_name}:")
        print(result.stdout)
        print(f"Standard Error for {script_name}:")
        print(result.stderr)

        # Basic assertions
        assert result.returncode == 0
