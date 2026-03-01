# Configuration files

## Supported file formats

Both config and data files can be written in JSON, JSON5, or YAML. The file extension determines the parser used:

| Format | Extensions | Notes |
|--------|-----------|-------|
| JSON | `.json` | Standard JSON. Parsed by the JSON5 parser, so comments and trailing commas are accepted even in `.json` files |
| JSON5 | `.json5` | Superset of JSON. Supports `//` and `/* */` comments, trailing commas, unquoted keys, and more |
| YAML | `.yml`, `.yaml` | Requires the optional `pyyaml` dependency (`uv sync --extra yaml`) |

JSON Schema validation files in `schema/` are always plain JSON and are not affected by this.

All inline examples in this document use JSON syntax as the universal baseline.

## `config.json`

### `filters`

A shared collection of filter definitions referenced by views. Each filter's `id` is used as the default column name to query against. If the target column differs from the `id`, use `query_columns` to specify the actual column name.

```json
"filters": [
    {
        "id": "filter_identifier",
        "label": "What you display on screen",
        "type": "select",
        "match": "exact"
    },
    {
        "id": "custom_filter_name",
        "label": "My label",
        "type": "select",
        "match": "prefix",
        "query_columns": {
            "column": "real_column_name"
        }
    }
]
```

#### Filter types

| `type` | `match` method | Additional attributes | Notes |
| ------ | -------------- | --------------------- | ----- |
| `select_list` | N/A | `filter_labels` (required) | Provide a list of distinct values with labels for user selection. `filter_labels` is a SQL expression used to generate the display label e.g. `upper(biotype)` |
| `select` | `exact`, `prefix` (required) | `query_columns` (optional) | Specify a single value to filter by. `exact` translates to `=` in SQL. `prefix` translates to `LIKE` with the backend appending `%` |
| `select_in` | `exact` (required) | `query_columns` (optional) | Specify multiple values to search by. Only exact matching is supported |
| `list_contains` | N/A | `query_columns` (optional) | Perform a `LIST_CONTAINS` function to find elements within an array column |
| `range` | N/A | `min`, `max` (optional), `query_columns` (optional) | Range query using `BETWEEN`. Specify `min` or `max` to override computed bounds |
| `location` | N/A | `query_columns` (required), `regex` (required) | Genomic coordinate overlap query. See [location filters](#location-filters) below |

#### `query_columns`

For most filter types, `query_columns` is optional and takes a single key:

```json
"query_columns": {
    "column": "actual_column_name"
}
```

Use this when the filter `id` differs from the column name you want to query. If omitted, the filter `id` is used as the column name.

When `query_columns` is explicitly provided, the ETL validates that the referenced columns exist in the data source.

#### Location filters

Location filters perform genomic-style coordinate overlap queries across multiple columns. The `query_columns` attribute is required and maps semantic roles to actual column names in the dataset. The `regex` attribute is also required and provides a regular expression with named capture groups for downstream consumers to parse location query strings.

```json
{
    "id": "gen_location",
    "label": "Genomic Location",
    "type": "location",
    "query_columns": {
        "region": "region",
        "start": "region_start",
        "end": "region_end",
        "strand": "strand",
        "bin": "_bin"
    },
    "regex": "(?P<region>[^:]+):(?P<start>\\d+)-(?P<end>\\d+)"
}
```

| Role | Required | Description |
|------|----------|-------------|
| `region` | Yes | Column representing the chromosome or region name |
| `start` | Yes | Column representing the feature start position |
| `end` | Yes | Column representing the feature end position |
| `strand` | No | Column representing strand (`+` / `-`). Omit if not available |
| `bin` | No | Column representing the UCSC extended bin index. Omit if not available |

The query finds overlapping features where the feature start is <= the query end and the feature end is >= the query start. If `bin` is provided, it is used to optimise the query using UCSC extended binning. If `strand` is provided and a strand value is given in the query, it will also filter by strand.

The `regex` field should use named capture groups matching the `query_columns` roles (except `bin`). Downstream consumers use this regex to parse location query strings into their component parts. The regex is stored as a separate column on `view_filter` in the database.

### `views`

```json
"views": [
    {
        "id": "view_one",
        "url_name": "url",
        "name": "View One",
        "source": "dataset_name",
        "include_remaining_columns": true,
        "filters": [
            { "filter_id": "column_one" },
            { "filter_id": "date" },
            {
                "group_id": "search",
                "group_label": "Search",
                "filters": [
                    { "filter_id": "name" },
                    { "filter_id": "category" }
                ]
            }
        ],
        "columns": [
            { "name": "column_one" },
            { "name": "column_ten" }
        ]
    }
]
```

Views configure what is presented to the user and differentiate content from the underlying data source.

| Attribute | Required | Description |
|-----------|----------|-------------|
| `id` | Yes | Unique identifier for this view |
| `url_name` | Yes | URL path segment for this view |
| `name` | Yes | Display name shown in the interface |
| `source` | Yes | Name of the data source (dataset) this view queries |
| `include_remaining_columns` | No | If `true`, append all remaining source columns not listed in `columns`. Defaults to `false` |
| `filters` | Yes | References to filters from the top-level `filters` array |
| `columns` | Yes | Columns to display, ordered by display rank (position in list determines order) |

#### View filters

Each entry in the view's `filters` array is either an individual filter reference or a filter group.

**Individual filter reference:**

| Attribute | Required | Description |
|-----------|----------|-------------|
| `filter_id` | Yes | References a filter `id` from the top-level `filters` array |

A standalone filter is automatically wrapped into a group of size 1, where the group inherits the filter's `id` and `label`.

**Filter group** — groups multiple filters together so the frontend can render them on a single panel:

```json
{
    "group_id": "gene_properties",
    "group_label": "Gene Properties",
    "filters": [
        { "filter_id": "species" },
        { "filter_id": "transcript_count" }
    ]
}
```

| Attribute | Required | Description |
|-----------|----------|-------------|
| `group_id` | Yes | Unique identifier for this group within the view |
| `group_label` | Yes | Display label for the group |
| `filters` | Yes | Array of individual filter references. Order determines rank within the group |

Groups are a view-level concern — the same filter definitions can be grouped differently across views.

#### Ranking

Display order is determined by two levels of ranking:

1. **Group rank** — position of the group (or standalone filter) in the view's `filters` array determines which group appears first
2. **Filter rank** — position within a group's `filters` array determines filter order within that group

In the database, groups are stored in `view_filter_group` with their rank, and filters in `view_filter` reference their group via `view_filter_group_id` with a within-group rank.

#### View columns

Columns are listed in display order. Position in the list determines rank (first column has rank 1, second has rank 2, etc.).

| Attribute | Required | Description |
|-----------|----------|-------------|
| `name` | Yes | Column name from the data source |
| `enabled` | No | If `true`, column is visible by default. Defaults to `true` |

### `columns`

```json
"columns": {
    "view_id": {
        "column_name": {
            "sortable": false,
            "type": "link",
            "url": "https://domain.org/path/{}"
        },
        "another": { ... }
    },
    "another_view_id": {
        ...
    }
}
```

Column overrides are specified per-view and control:

- If you are allowed to sort on the column
- The type of column data represented
- Additional attributes to help rendering
- Custom labels
- If the column should be hidden from direct querying and display

| Attribute | Data type | Notes |
|-----------|-----------|-------|
| `label` | `string` | Custom label for the column. Use to override the default generated display name |
| `sortable` | `boolean` | Indicates if sorting is allowed on this column |
| `hidden` | `boolean` | If true, column is recorded in the database but excluded from direct querying and display in the interface. The column remains available to the query engine for operations such as location filtering |
| `type` | `string` | Link rendering type. Supported types are `link`, `array-link`, `labelled-link` |
| `url` | `string` | URL template used with `link` and `array-link` types. Format is `scheme://domain/path/{}` where `{}` is substituted with the cell's content. Required when `type` is `link` or `array-link` |
| `delimiter` | `string` | Delimiter for splitting values when using the `array-link` type. Required when `type` is `array-link` |

#### The different types of links

We support three types of links in the interface. They are:

1. `link`: use the `url` field to generate a link out. The cell's content is used as the linkable text
2. `array-link`: use the `delimiter` to split the contents into multiple values and apply the same scheme as in `link`
3. `labelled-link`: the cell's content looks like `label|url` and will be split on the `|` character to generate a `href`
