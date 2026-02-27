-- View definitions
CREATE TABLE "view" (
    view_id INTEGER PRIMARY KEY,
    id VARCHAR NOT NULL UNIQUE,
    url_name VARCHAR NOT NULL UNIQUE,
    "name" VARCHAR NOT NULL,
    "source" VARCHAR NOT NULL
);

-- Filter groups linked to a view
CREATE TABLE view_filter_group (
    view_filter_group_id INTEGER PRIMARY KEY,
    view_id INTEGER NOT NULL,
    "id" VARCHAR NOT NULL,
    "label" VARCHAR NOT NULL,
    rank INTEGER NOT NULL,
    FOREIGN KEY (view_id) REFERENCES view(view_id),
    UNIQUE(view_id, "id")
);

-- Filter definitions linked to a view via a group
CREATE TABLE view_filter (
    view_filter_id INTEGER PRIMARY KEY,
    view_filter_group_id INTEGER NOT NULL,
    "id" VARCHAR NOT NULL,
    "label" VARCHAR NOT NULL,
    filter_type VARCHAR NOT NULL,
    match_type VARCHAR,
    rank INTEGER NOT NULL,
    "min" DOUBLE,
    "max" DOUBLE,
    query_columns JSON,
    regex VARCHAR,
    UNIQUE("id"),
    FOREIGN KEY (view_filter_group_id) REFERENCES view_filter_group(view_filter_group_id)
);

-- Pre-computed filter values (only populated for select_list type)
CREATE TABLE view_filter_value (
    view_filter_id INTEGER NOT NULL,
    "value" VARCHAR NOT NULL,
    "label" VARCHAR NOT NULL,
    FOREIGN KEY (view_filter_id) REFERENCES view_filter(view_filter_id),
    UNIQUE(view_filter_id, value, label)
);

-- Column metadata and view association (merged column_def + view_column)
CREATE TABLE view_column (
    view_column_id INTEGER PRIMARY KEY,
    view_id INTEGER NOT NULL,
    "name" VARCHAR NOT NULL,
    "label" VARCHAR NOT NULL,
    "type" VARCHAR NOT NULL,
    sortable BOOLEAN NOT NULL DEFAULT true,
    url VARCHAR,
    "delimiter" VARCHAR,
    hidden BOOLEAN NOT NULL DEFAULT false,
    rank INTEGER NOT NULL,
    enable_by_default BOOLEAN NOT NULL DEFAULT true,
    FOREIGN KEY (view_id) REFERENCES "view"(view_id),
    UNIQUE (view_id, "name")
);

-- Release metadata
CREATE TABLE IF NOT EXISTS "release" (
    release_label VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL
);

-- Convenience view: resolves view filters with their config
CREATE VIEW filter_config AS
SELECT
    v.view_id as view_dbid,
    v.id as view_id,
    v.url_name AS view_url_name,
    v."name" AS view_name,
    v."source" AS source,
    vfg."id" AS group_id,
    vfg."label" AS group_label,
    vfg.rank AS group_rank,
    vf.rank AS filter_rank,
    vf.view_filter_id,
    vf."id" AS filter_name,
    vf."label" AS filter_label,
    vf.filter_type,
    vf.match_type,
    vf."min",
    vf."max",
    vf.query_columns,
    vf.regex
FROM view_filter vf
    JOIN view_filter_group vfg ON vf.view_filter_group_id = vfg.view_filter_group_id
    JOIN "view" v ON vfg.view_id = v.view_id
ORDER BY v.view_id, vfg.rank, vf.rank;

-- Convenience view: resolves view columns with their metadata
CREATE VIEW column_config AS
SELECT
    v.view_id,
    v.url_name AS view_url_name,
    v."name" AS view_name,
    v."source" AS source,
    vc.rank AS column_rank,
    vc.enable_by_default,
    vc.hidden,
    vc."name" AS column_name,
    vc."label" AS column_label,
    vc."type" AS column_type,
    vc.sortable,
    vc.url,
    vc."delimiter"
FROM view_column vc
    JOIN "view" v ON vc.view_id = v.view_id
ORDER BY v.view_id, vc.rank;
