import json
import logging
from pathlib import Path

import duckdb

from etl.models import (
    Dataset,
    Filter,
    View,
    ViewColumn,
    ViewFilter,
    ViewFilterGroup,
)

logger = logging.getLogger(__name__)


class FilterError(Exception):
    pass


class ViewsProcessor:
    """Creates the filters available by querying distinct values from each dataset and
    parquet file. Saves to JSON files in the release_path.

    Filters are based on the filters defined in the config file. We also apply
    the filter_label text in the SQL select to get the label for each filter value. You can
    configure this to be different from the cell contents if needed e.g. lowercasing, trimming, adding prefixes etc.

    Attributes:
        - datasets: list[Dataset] of datasets available to link to a view
        - views: list[View] of views to process
        - filters: list[Filter] of all available filters
        - release_path: Path where to write data to
        - warn_max: int, maximum number of distinct filter values before warning. Default 60
    """  # noqa: E501

    def __init__(
        self,
        views: list[View],
        filters: list[Filter],
        datasets: list[Dataset],
        release_path: Path,
        warn_max: int = 60,
    ):
        self.views = views
        self.filters = filters
        self.datasets = datasets
        self.release_path = release_path
        self.warn_max = warn_max

    def run(self) -> None:
        with duckdb.connect() as conn:
            for view in self.views:
                dataset = self.get_dataset(view.dataset)
                normalised_groups = self.normalise_to_groups(view)
                group_rank = 1
                for group in normalised_groups:
                    group.rank = group_rank
                    group_rank += 1
                    filter_rank = 1
                    for view_filter in group.filters:
                        self.process_filter(
                            view, view_filter, dataset.parquet_path, conn
                        )
                        view_filter.rank = filter_rank
                        filter_rank += 1
                    # For auto-wrapped groups (size 1), use the filter's
                    # resolved label as the group label
                    if len(group.filters) == 1 and group.filters[0].label:
                        group.group_label = group.filters[0].label
                # Replace view.filters with the normalised groups
                view.filters = normalised_groups
                self.populate_additional_columns(view)
                self.write_view(view)

    def normalise_to_groups(self, view: View) -> list[ViewFilterGroup]:
        """Wrap standalone ViewFilters into single-filter ViewFilterGroups.

        For standalone filters, the group inherits the filter's id and label
        (label is resolved later during process_filter via copy_from_filter).
        """
        groups: list[ViewFilterGroup] = []
        for entry in view.filters:
            if isinstance(entry, ViewFilterGroup):
                groups.append(entry)
            else:
                # Auto-wrap: group_id and group_label will be set after
                # the filter definition is resolved
                groups.append(
                    ViewFilterGroup(
                        group_id=entry.filter_id,
                        group_label=entry.filter_id,
                        filters=[entry],
                    )
                )
        return groups

    def process_filter(
        self,
        view: View,
        view_filter: ViewFilter,
        parquet: str | Path | None,
        conn: duckdb.DuckDBPyConnection,
    ) -> None:
        filter_definition = self.get_filter_definition(view, view_filter)
        if filter_definition.type == "select_list":
            filter_values = self.distinct_filter_values(
                filter_definition, parquet, conn
            )
            size = len(filter_values)
            if size == 0:
                logging.warning(
                    f"Problem. No values found for {filter_definition.id!r}"
                )
            elif size > self.warn_max:
                logging.warning(
                    f"{view.dataset} - {filter_definition.id!r} has over {self.warn_max} ({size}) values"  # noqa: E501
                )
            view_filter.filter_values = filter_values
        view_filter.copy_from_filter(filter_definition)

    def get_filter_definition(self, view: View, view_filter: ViewFilter) -> Filter:
        for filter in self.filters:
            if filter.id == view_filter.filter_id:
                return filter
        raise FilterError(
            f"Cannot find the filter '{view_filter.filter_id}' in the view '{view.name}'"  # noqa: E501
        )

    def get_dataset(self, dataset_name: str) -> Dataset:
        for dataset in self.datasets:
            if dataset.name == dataset_name:
                return dataset
        raise FilterError(f"No dataset found for '{dataset_name}'")

    def distinct_filter_values(
        self,
        filter: Filter,
        parquet: str | Path | None,
        conn: duckdb.DuckDBPyConnection,
    ) -> list[dict[str, str]]:
        distinct_sql = f"""
SELECT DISTINCT "{filter.target_column}" AS value, {filter.filter_labels} AS label
from '{parquet}'
WHERE "{filter.target_column}" IS NOT NULL
ORDER BY label ASC
"""
        logger.debug(distinct_sql)
        results = conn.sql(distinct_sql)
        columns = results.columns
        fetch_results = results.fetchall()
        filter_values = []
        for r in fetch_results:
            filter_values.append({columns[0]: str(r[0]), columns[1]: str(r[1])})
        return filter_values

    def populate_additional_columns(self, view: View) -> None:
        rank = 1
        seen: dict[str, bool] = {}
        # Rank existing columns and record we have seen them
        for column in view.columns:
            column.rank = rank
            rank = rank + 1
            seen[column.name] = True

        dataset_columns = self.get_dataset(view.dataset).columns
        if dataset_columns is None:
            return
        for ds_column in dataset_columns:
            if ds_column.name is not None and ds_column.name not in seen:
                if not ds_column.hidden:
                    view.columns.append(ViewColumn(name=ds_column.name, rank=rank))
                    rank = rank + 1

    def write_view(self, view: View) -> Path:
        save_path = self.release_path / f"view-{view.id}.json"
        with open(save_path, "w") as fh:
            json.dump(view.model_dump(exclude_none=True), fh, indent=4)
        return save_path
