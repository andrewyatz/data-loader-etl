import json
from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def schema_dir():
    return Path(__file__).resolve().parent.parent / "schema"


@pytest.fixture
def data_dir():
    return Path(__file__).resolve().parent / "data"


@pytest.fixture
def config_dir():
    return Path(__file__).resolve().parent / "config"


@pytest.fixture
def sql_dir():
    return Path(__file__).resolve().parent.parent / "sql"


@pytest.fixture
def results_dir():
    return Path(__file__).resolve().parent / "results"


@pytest.fixture
def sql_version():
    return "v2"


@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def data_json(tmp_path, config_dir, data_dir):
    rewritten_json_path = tmp_path / "data.json"
    with open(config_dir / "data.json", "rt") as fh:
        blob = json.load(fh)
        blob[0]["path"] = str(data_dir / "test.csv")
        with open(rewritten_json_path, "wt") as wfh:
            json.dump(blob, wfh, indent=2)

    return rewritten_json_path
