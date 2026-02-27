import json
from pathlib import Path

import duckdb

from etl.models import Column, Dataset, View


class DatasetsProcessorError(Exception):
    pass


class DatasetsProcessor:
    """Create configuration for datasets for later use. To do this it

    - Introspects parquet files for column names per dataset
    - Creates a config for all columns including reformatting column names
    - Column overrides (keyed by view id) are applied later by ViewsProcessor
    - Saving to JSON in the release_path

    Attributes:
      - datasets: list[Dataset] of datasets to process
      - views: list[View] of views referencing datasets
      - columns: dict[str,dict[str,Column]] of configs to apply, keyed by view id
      - release_path: where to write data to
    """  # noqa: E501

    def __init__(
        self,
        datasets: list[Dataset],
        views: list[View],
        columns: dict[str, dict[str, Column]],
        release_path: Path,
    ):
        self.datasets = datasets
        self.views = views
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
                conn.read_parquet(str(parquet_path))
                raw_columns = self.get_columns(parquet_path, conn)

                # Build base column metadata (no view-specific overrides)
                base_columns = self._build_base_columns(raw_columns)
                dataset.columns = base_columns

                # Validate that per-view column overrides reference real columns
                real_names = {c[0] for c in raw_columns}
                for view in self.views:
                    if view.source != name:
                        continue
                    view_overrides = self.columns.get(view.id, {})
                    for col_name in view_overrides:
                        if col_name not in real_names:
                            raise DatasetsProcessorError(
                                f"Column '{col_name}' specified in columns config "
                                f"for view '{view.id}' is not found in "
                                f"source '{name}'"
                            )

                # Write dataset metadata JSON
                dataset.parquet_path = None
                metadata_path = self._write_dataset(dataset)
                dataset.column_metadata_path = metadata_path
                dataset.parquet_path = parquet_path

    def _build_base_columns(
        self, raw_columns: list[tuple[str, str]]
    ) -> list[Column]:
        """Build Column objects from parquet introspection with default labels."""
        column_output = []
        for column_name, _ in raw_columns:
            column_config = Column()
            column_config.name = column_name
            label = column_name.replace("_", " ")
            label = label[0].upper() + label[1:]
            column_config.label = label
            column_output.append(column_config)
        return column_output

    def _write_dataset(self, dataset: Dataset) -> Path:
        save_path = self.release_path / f"dataset-{dataset.name}.json"
        with open(save_path, "w") as fh:
            json.dump(dataset.model_dump(exclude_none=True), fh, indent=4)
        return save_path

    def get_columns(
        self, parquet_path: Path, conn: duckdb.DuckDBPyConnection
    ) -> list[tuple[str, str]]:
        sql = f"SELECT column_name, column_type FROM (describe'{str(parquet_path)}')"
        return conn.sql(sql).fetchall()
