# import pytest
import logging
import sys

import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

from etl.main import run_etl

log = logging.getLogger(__name__)


def test_run_etl(
    monkeypatch, tmp_path, config_dir, data_dir, schema_dir, results_dir, data_json
):
    release = "test_v1"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--release",
            release,
            "--config",
            str(config_dir / "config.json"),
            "--data",
            str(data_json),
            "--schema",
            str(schema_dir),
        ],
    )
    monkeypatch.chdir(tmp_path)
    run_etl()
    log.info(tmp_path)

    duckdb_path = tmp_path / release / f"{release}.duckdb"
    assert duckdb_path.exists()
    assert duckdb_path.is_file()

    #     # input()
    con = duckdb.connect(duckdb_path)
    # Assert the test table
    _assert_table(
        results_dir, con, "test", numeric_cols=["start", "stop", "measurement"]
    )

    # Now assert the ancillary tables
    remaining_tables = [
        "dataset",
        "column_def",
        "view",
        "view_column",
        "view_filter",
        "view_filter_value",
    ]
    for table in remaining_tables:
        _assert_table(results_dir, con, table)

    # Assert the release table alone since it has different data that changes on date
    expected = pd.DataFrame({"release_label": [pd.Timestamp.now().strftime("%Y-%m")]})
    actual = con.execute("SELECT release_label FROM release").fetchdf()
    assert_frame_equal(actual, expected, check_dtype=False)


def _assert_table(results_dir, con, table_name, numeric_cols=[]):
    print(f"Asserting table: {table_name}")
    expected = pd.read_csv(results_dir / f"{table_name}.csv")
    actual = con.execute(
        f"select {",".join(expected.columns)} from {table_name}"
    ).fetchdf()
    for col in numeric_cols:
        expected[col] = pd.to_numeric(expected[col])
        actual[col] = pd.to_numeric(actual[col])
    for col in actual.columns:
        actual[col] = actual[col].astype(expected[col].dtype)
    assert_frame_equal(actual, expected, check_dtype=False)
