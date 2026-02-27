import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import duckdb

from etl.models import View, ViewFilterGroup

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

    def __init__(
        self,
        release_path: Path,
        release: str,
        views: list[View],
        schema_version: str = schema_version,
    ):
        BaseDatabase.__init__(self, release_path, release)
        self.views = views
        self.schema_version = schema_version
        self.ids: dict[str, int] = {}

    def run(self) -> None:
        self.load_schema()
        for view in self.views:
            logging.info(f"Processing {view.name}, filters and columns")
            self.write_view(view)
            logging.info("Finished")
        self.generate_release()

    def write_view(self, view: View) -> None:
        conn = self.conn
        view_db_id = self.next_id("view")

        # Define the view with source instead of dataset_id
        conn.execute(
            'INSERT INTO "view" (view_id, id, name, url_name, source) VALUES (?,?,?,?,?)',  # noqa: E501
            (view_db_id, view.id, view.name, view.url_name, view.source),
        )

        # Write filter groups and their filters
        # After view processing, view.filters is a list[ViewFilterGroup]
        for group in view.filters:
            assert isinstance(group, ViewFilterGroup)
            group_db_id = self.next_id("view_filter_group")
            group_sql = "INSERT INTO view_filter_group (view_filter_group_id, view_id, id, label, rank) VALUES (?,?,?,?,?)"  # noqa: E501
            conn.execute(
                group_sql,
                (group_db_id, view_db_id, group.group_id, group.group_label, group.rank),
            )

            for view_filter in group.filters:
                view_filter_db_id = self.next_id("view_filter")
                filter_sql = "INSERT INTO view_filter (view_filter_id, view_filter_group_id, id, label, filter_type, match_type, rank, min, max, query_columns, regex) VALUES (?,?,?,?,?,?,?,?,?,?,?)"  # noqa: E501
                # Serialize query_columns if it's a Pydantic model
                qc = view_filter.query_columns
                if qc is not None and hasattr(qc, "model_dump"):
                    qc = qc.model_dump(exclude_none=True)
                filter_params = (
                    view_filter_db_id,
                    group_db_id,
                    view_filter.filter_id,
                    view_filter.label,
                    view_filter.type,
                    view_filter.match,
                    view_filter.rank,
                    view_filter.min,
                    view_filter.max,
                    qc,
                    view_filter.regex,
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

        # Write merged columns (column metadata + view association)
        for column in view.columns:
            col_db_id = self.next_id("view_column")
            col_sql = "INSERT INTO view_column (view_column_id, view_id, name, label, type, sortable, url, delimiter, hidden, rank, enable_by_default) VALUES (?,?,?,?,?,?,?,?,?,?,?)"  # noqa: E501
            col_params = (
                col_db_id,
                view_db_id,
                column.name,
                column.label,
                column.type,
                column.sortable,
                column.url,
                column.delimiter,
                column.hidden,
                column.rank,
                column.enabled,
            )
            conn.execute(col_sql, col_params)

    def generate_release(self) -> None:
        sql = "INSERT INTO release (release_label, schema_version) VALUES (strftime(current_date(),'%Y-%m'), ?)"  # noqa: E501
        self.conn.execute(sql, (self.schema_version,))

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
