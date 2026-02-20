import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import duckdb

from etl.models import Dataset, View

logger = logging.getLogger(__name__)

schema_version = "v2"


class BaseDatabase:
    def __init__(self, release_path: Path, release: str) -> None:
        self.release_path = release_path
        self.release = release
        self.conn: duckdb.DuckDBPyConnection

    def __enter__(self) -> Self:
        path = self.release_path / f"{self.release}.duckdb"
        self.conn = duckdb.connect(str(path))
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        self.conn.close()


class DatabaseConfig(BaseDatabase):

    def run(self) -> None:
        self.load_schema()
        for dataset in self.datasets:
            logging.info(f"Processing dataset {dataset.name} and columns")
            self.write_dataset(dataset)
            logging.info("Finished")
        for view in self.views:
            logging.info(f"Processing {view.name}, filters and columns")
            self.write_view(view)
            logging.info("Finished")
        self.generate_release()

    def write_dataset(self, dataset: Dataset) -> None:
        conn = self.conn
        dataset_db_id = self.next_id("dataset")
        self._dataset_lookup[dataset.name] = dataset_db_id
        conn.execute(
            "INSERT INTO dataset (dataset_id, name) VALUES (?,?)",
            (dataset_db_id, dataset.name),
        )
        if dataset.columns is None:
            return
        for column in dataset.columns:
            column_db_id = self.next_id("column_def")
            fullname = f"{dataset.name}-{column.name}"
            sql = "INSERT INTO column_def (column_id, dataset_id, name, label, type, sortable, url, delimiter) VALUES (?,?,?,?,?,?,?,?)"  # noqa: E501
            params = (
                column_db_id,
                dataset_db_id,
                column.name,
                column.label,
                column.type,
                column.sortable,
                column.url,
                column.delimiter,
            )
            conn.execute(sql, params)
            self._column_lookup[fullname] = column_db_id

    def write_view(self, view: View) -> None:
        conn = self.conn
        view_db_id = self.next_id("view")
        dataset_db_id = self._dataset_lookup[view.dataset]

        # Define the view
        conn.execute(
            "INSERT INTO view (view_id, id, name, url_name, dataset_id) VALUES (?,?,?,?,?)",  # noqa: E501
            (view_db_id, view.id, view.name, view.url_name, dataset_db_id),
        )

        # Write all filters out
        for view_filter in view.filters:
            view_filter_db_id = self.next_id("view_filter")
            filter_sql = "INSERT INTO view_filter (view_filter_id, view_id, id, label, filter_type, match_type, is_primary, rank, min, max, query_columns) VALUES (?,?,?,?,?,?,?,?,?,?,?)"  # noqa: E501
            filter_params = (
                view_filter_db_id,
                view_db_id,
                view_filter.filter_id,
                view_filter.label,
                view_filter.type,
                view_filter.match,
                view_filter.primary,
                view_filter.rank,
                view_filter.min,
                view_filter.max,
                view_filter.query_columns,
            )
            conn.execute(filter_sql, filter_params)
            if (
                view_filter.type == "select_list"
                and view_filter.filter_values is not None
            ):
                for value in view_filter.filter_values:
                    value_sql = "INSERT INTO view_filter_value (view_filter_id, value, label) VALUES (?,?,?)"  # noqa: E501
                    value_params = (
                        view_filter_db_id,
                        value["value"],
                        value["label"],
                    )
                    conn.execute(value_sql, value_params)

        # Write the columns for the view and link
        for column in view.columns:
            fullname = f"{view.dataset}-{column.name}"
            column_id = self._column_lookup[fullname]
            col_sql = "INSERT INTO view_column (view_id, column_id, rank, enable_by_default) VALUES (?,?,?,?)"  # noqa: E501
            col_params = (
                view_db_id,
                column_id,
                column.rank,
                column.enabled,
            )
            conn.execute(col_sql, col_params)

    def generate_release(self) -> None:
        sql = "INSERT INTO release (release_label, schema_version) VALUES (strftime(current_date(),'%Y-%m'), ?)"  # noqa: E501
        self.conn.execute(sql, (self.schema_version,))

    def __init__(
        self,
        release_path: Path,
        release: str,
        datasets: list[Dataset],
        views: list[View],
        schema_version: str = schema_version,
    ):
        BaseDatabase.__init__(self, release_path, release)
        self.datasets = datasets
        self.views = views
        self.schema_version = schema_version
        self._dataset_lookup: dict[str, int] = {}
        self._column_lookup: dict[str, int] = {}
        self._view_lookup: dict[str, int] = {}
        self.ids: dict[str, int] = {}

    def load_schema(self) -> None:
        conn = self.conn
        path = (
            Path(__file__).parent.parent / "sql" / f"schema.{self.schema_version}.sql"
        )
        with open(path, "rt") as fh:
            content = fh.read()
            conn.execute(content)
        logging.info(f"Database schema loaded from {path!r}")

    def get_files(self, prefix: str) -> list[str]:
        """Take a prefix, find all files in the release_path directory
        and load JSON content for further processing"""
        loaded = []
        for file in self.release_path.iterdir():
            if (
                file.is_file()
                and file.name.startswith(f"{prefix}-")
                and file.name.endswith(".json")
            ):
                with open(file, "rt") as fh:
                    content = fh.read()
                    loaded.append(content)
        return loaded

    def next_id(self, table: str) -> int:
        current_id = self.ids.get(table, 1)
        self.ids[table] = current_id + 1
        return current_id


class Database(BaseDatabase):
    """Copies the parquet files present in the release directory into the DuckDB
    database. Looks for all .parquet files in the release_path and creates a table
    for each file named after the file stem.
    """

    def run(self) -> None:
        for path in self.get_parquet_paths():
            self.load_parquet(path)

    def load_parquet(self, path: Path) -> None:
        table_name = path.stem
        logging.info(f"Loading parquet {path!r} into table {table_name!r}")
        self.conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')"
        )

    def get_parquet_paths(self) -> Iterator[Path]:
        for file in self.release_path.iterdir():
            if file.is_file() and file.name.endswith(".parquet"):
                yield file
