"""Pydantic models for config.json and data.json, aligned with schema v2."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Union

from pydantic import BaseModel

# ── Filter models ──


class SingleQueryColumn(BaseModel):
    column: str


class LocationQueryColumns(BaseModel):
    region: str
    start: str
    end: str
    strand: str | None = None
    bin: str | None = None


class Filter(BaseModel):
    id: str
    label: str
    type: Literal[
        "select_list", "select", "select_in", "list_contains", "range", "location"
    ]
    match: Literal["exact", "prefix"] | None = None
    filter_labels: str | None = None
    min: float | None = None
    max: float | None = None
    query_columns: SingleQueryColumn | LocationQueryColumns | None = None
    regex: str | None = None

    @property
    def target_column(self) -> str:
        """The actual column name to query against."""
        if isinstance(self.query_columns, SingleQueryColumn):
            return self.query_columns.column
        return self.id


# ── View models ──


class ViewFilter(BaseModel):
    filter_id: str

    # Attributes we copy across from the Filter object definition above
    # which is why we're not very tight on the defs
    label: str | None = None
    type: (
        Literal[
            "select_list", "select", "select_in", "list_contains", "range", "location"
        ]
        | None
    ) = None
    match: Literal["exact", "prefix"] | None = None
    min: float | None = None
    max: float | None = None
    rank: int | None = None
    filter_values: list[dict[str, str]] | None = None
    query_columns: SingleQueryColumn | LocationQueryColumns | None = None
    regex: str | None = None

    def copy_from_filter(self, filter: Filter) -> None:
        for key, value in filter.model_dump(exclude_none=True).items():
            if hasattr(self, key):
                setattr(self, key, value)


class ViewFilterGroup(BaseModel):
    group_id: str
    group_label: str
    rank: int | None = None
    filters: list[ViewFilter]


class ViewColumn(BaseModel):
    name: str
    enabled: bool = True
    rank: int | None = None
    # Populated during processing from dataset introspection + config overrides
    label: str | None = None
    sortable: bool = True
    hidden: bool = False
    type: Literal["link", "array-link", "labelled-link", "string"] = "string"
    url: str | None = None
    delimiter: str | None = None


class View(BaseModel):
    url_name: str
    id: str
    name: str
    source: str
    include_remaining_columns: bool = False
    filters: list[Union[ViewFilterGroup, ViewFilter]]
    columns: list[ViewColumn]


# ── Columns ──


class Column(BaseModel):
    name: str | None = None
    label: str | None = None
    sortable: bool = True
    hidden: bool | None = False
    type: Literal["link", "array-link", "labelled-link", "string"] = "string"
    url: str | None = None
    delimiter: str | None = None


# ── Top-level config ──


class Config(BaseModel):
    filters: list[Filter]
    views: list[View]
    columns: dict[str, dict[str, Column]] = {}  # keyed by view id

    def get_filter(self, filter_id: str) -> Filter:
        for f in self.filters:
            if f.id == filter_id:
                return f
        raise KeyError(f"Unknown filter id: {filter_id!r}")


# ── Dataset models (data.json) ──


class CreateColumn(BaseModel):
    name: str
    command: str


class Dataset(BaseModel):
    name: str
    path: str
    parquet_path: str | Path | None = None
    filter: str | None = None
    filter_column: str | None = None
    create_columns: list[CreateColumn] = []
    column_metadata_path: str | Path | None = None
    columns: list[Column] | None = None
