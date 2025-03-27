# tests/conftest.py
import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    os.environ["HOMEobsforge"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    pythonpath = os.environ.get("PYTHONPATH", "")
    sorc_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../sorc/wxflow/src"))
    os.environ["PYTHONPATH"] = f"{pythonpath}:{sorc_path}"
    os.environ["PYTHONPATH"] += f":{os.path.abspath(os.path.join(os.path.dirname(__file__), '../../ush/python'))}"
    os.environ["CONFIGYAML"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yaml"))


@pytest.fixture(autouse=True, scope="session")
def isolate_test_output():
    home_obsforge = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    test_dir = os.path.join(home_obsforge, 'scripts', 'tests', 'tests_output')
    run_dir = os.path.join(test_dir, 'RUNDIRS', 'obsforge')
    os.makedirs(test_dir, exist_ok=True)
    os.makedirs(os.path.join(run_dir), exist_ok=True)

    os.chdir(os.path.join(run_dir))
