import os
from typing import Any

from jsonschema import validate

supported_filters = [
    "select",
    "select_in",
    "select_list",
    "range",
    "location",
    "list_contains",
]


class ConfigError(Exception):
    pass


def _mark_filter_used(
    filter: dict[str, Any], view: dict[str, Any], used_filters: dict[str, bool]
) -> None:
    filter_id = filter["filter_id"]
    if filter_id not in used_filters:
        raise ConfigError(f"Unknown filter: {filter_id} found for {view['name']}")
    else:
        used_filters[filter_id] = True


def validate_config(config: dict[str, Any], schema: dict[str, Any]) -> None:
    # Validate JSON
    validate(instance=config, schema=schema)

    # Validate filters
    used_filters = {}
    for f in config["filters"]:
        if f["type"] not in supported_filters:
            raise ConfigError(
                f"Unknown filter type: '{type}'. Supported types are {supported_filters!r}"  # noqa: E501
            )
        filter_id = f["id"]
        if filter_id in used_filters:
            raise ConfigError(
                f"Filter id '{filter_id}' is not unique. Filters must have a unique id"
            )
        used_filters[f["id"]] = False

    # Check filter names (walking into groups)
    for view in config["views"]:
        group_ids: set[str] = set()
        for entry in view["filters"]:
            if "group_id" in entry:
                # Filter group
                gid = entry["group_id"]
                if gid in group_ids:
                    raise ConfigError(
                        f"Duplicate group_id '{gid}' in view '{view['name']}'"
                    )
                group_ids.add(gid)
                for filter in entry["filters"]:
                    _mark_filter_used(filter, view, used_filters)
            else:
                _mark_filter_used(entry, view, used_filters)

    # check for unused filter
    for f, used in used_filters.items():
        if not used:
            raise ConfigError(f"Filter {f} is not used by any of the views!")


def validate_dataset(datasets: list[dict[str, Any]], schema: dict[str, Any]) -> None:
    validate(instance=datasets, schema=schema)
    for dataset in datasets:
        if not os.path.exists(dataset["path"]):
            raise ConfigError(f"Unable to access {dataset['path']}")
