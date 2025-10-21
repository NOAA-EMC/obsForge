# tests/conftest.py
import os
import pytest

home_obsforge = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
test_dir = os.path.join(home_obsforge, 'scripts', 'tests', 'tests_output')
run_dir = os.path.join(test_dir, 'RUNDIRS', 'obsforge')
comroot = os.path.join(test_dir, 'COMROOT')
dcomroot = os.path.join(test_dir, 'dcom')
dataroot = os.path.join(test_dir, 'RUNDIRS')


@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    os.environ["HOMEobsforge"] = home_obsforge
    pythonpath = os.environ.get("PYTHONPATH", "")
    sorc_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../sorc/wxflow/src"))
    os.environ["PYTHONPATH"] = f"{pythonpath}:{sorc_path}"
    os.environ["PYTHONPATH"] += f":{os.path.abspath(os.path.join(os.path.dirname(__file__), '../../ush/python'))}"
    os.environ["CONFIGYAML"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yaml"))
    os.environ["DATA"] = run_dir
    os.environ["COMROOT"] = comroot
    os.environ["DCOMROOT"] = dcomroot
    os.environ["DATAROOT"] = dataroot


@pytest.fixture(autouse=True, scope="session")
def isolate_test_output():
    test_dir = os.path.join(home_obsforge, 'scripts', 'tests', 'tests_output')
    run_dir = os.path.join(test_dir, 'RUNDIRS', 'obsforge')
    os.environ["DATA"] = run_dir
    os.makedirs(test_dir, exist_ok=True)
    os.makedirs(os.path.join(run_dir), exist_ok=True)

    os.chdir(os.path.join(run_dir))
