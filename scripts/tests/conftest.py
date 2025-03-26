# tests/conftest.py
import os
import pytest

@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    os.environ["HOMEobsforge"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    os.environ["PYTHONPATH"] = os.environ.get("PYTHONPATH", "") + f":{os.path.abspath(os.path.join(os.path.dirname(__file__), '../../sorc/wxflow/src'))}"
    os.environ["PYTHONPATH"] += f":{os.path.abspath(os.path.join(os.path.dirname(__file__), '../../ush/python'))}"