import json
from pathlib import Path

import duckdb

from etl.models import Column, Dataset


class DatasetsProcessorError(Exception):
    pass


class DatasetsProcessor:
    """Create configuration for datasets for later use. To do this it

    - Combines columns configuration specified in the config.json file
    - Loops through all datasets
    - Loads parquet and queries for the column names
    - Creates a config for all columns including reformatting column names to something more readable
    - You can oveerride this via the columns lookup
    - Saving to JSON in the release_path

    Attributes:
      - datasets: list[Dataset] of datasets to process
      - columns: dict[str,dict[str,Column]] of configs to apply for columns. If a column is missing we create a default one
      - release_path: where to write data to
    """  # noqa: E501

    def __init__(
        self,
        datasets: list[Dataset],
        columns: dict[str, dict[str, Column]],
        release_path: Path,
    ):
        self.datasets = datasets
        self.columns = columns
        self.release_path = release_path

    def run(self) -> None:

        with duckdb.connect() as conn:
            for dataset in self.datasets:
                name = dataset.name
                parquet_path = dataset.parquet_path
                if parquet_path is None:
                    raise DatasetsProcessorError(
                        f"Dataset '{name}' has no parquet_path"
                    )
                parquet_path = Path(parquet_path)
                columns = self.columns.get(name, {})
                new_columns = self.process_dataset(name, parquet_path, columns, conn)
                dataset.columns = new_columns
                # This is nasty just remove and re-add. It's easier
                # because POSIX path doesn't write
                dataset.parquet_path = None
                metadata_path = self.write_dataset(dataset)
                dataset.column_metadata_path = metadata_path
                dataset.parquet_path = parquet_path

    def write_dataset(self, dataset: Dataset) -> Path:
        save_path = self.release_path / f"dataset-{dataset.name}.json"
        with open(save_path, "w") as fh:
            json.dump(dataset.model_dump(exclude_none=True), fh, indent=4)
        return save_path

    def process_dataset(
        self,
        name: str,
        parquet_path: Path,
        columns: dict[str, Column],
        conn: duckdb.DuckDBPyConnection,
    ) -> list[Column]:
        column_output = []
        conn.read_parquet(str(parquet_path))
        results = self.get_columns(parquet_path, conn)
        column_exists = {}
        for row in results:
            column_name, _ = row
            if column_name in columns:
                column_config = columns[column_name].model_copy(deep=True)
            else:
                column_config = Column()
            column_config.name = column_name
            # Build a label if there isn't one
            if column_config.label is None:
                label = column_name.replace("_", " ")
                label = label[0].upper() + label[1:]
                column_config.label = label
            column_output.append(column_config)
            column_exists[column_name] = True

        # Validate that all columns are present in the dataset
        for column_name in columns.keys():
            if column_name not in column_exists:
                raise DatasetsProcessorError(
                    f"Column '{column_name}' specified in columns config is not found in dataset {name}"  # noqa: E501
                )

        return column_output

    def get_columns(
        self, parquet_path: Path, conn: duckdb.DuckDBPyConnection
    ) -> list[tuple[str, str]]:
        sql = f"SELECT column_name, column_type FROM (describe'{str(parquet_path)}')"
        return conn.sql(sql).fetchall()
