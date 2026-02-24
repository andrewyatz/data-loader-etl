-- Dataset registry
CREATE TABLE dataset (
    dataset_id INTEGER PRIMARY KEY,
    "name" VARCHAR NOT NULL UNIQUE
);

-- Column metadata, scoped to a dataset
CREATE TABLE column_def (
    column_id INTEGER PRIMARY KEY,
    dataset_id INTEGER NOT NULL,
    "name" VARCHAR NOT NULL,
    "label" VARCHAR NOT NULL,
    "type" VARCHAR NOT NULL,
    sortable BOOLEAN NOT NULL DEFAULT true,
    url VARCHAR,
    "delimiter" VARCHAR,
    FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id),
    UNIQUE (dataset_id, "name")
);

-- View definitions
CREATE TABLE "view" (
    view_id INTEGER PRIMARY KEY,
    id VARCHAR NOT NULL UNIQUE,
    url_name VARCHAR NOT NULL UNIQUE,
    "name" VARCHAR NOT NULL,
    dataset_id INTEGER NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id)
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

-- Links columns to views with display ordering
CREATE TABLE view_column (
    view_id INTEGER NOT NULL,
    column_id INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    enable_by_default BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (view_id, column_id),
    FOREIGN KEY (view_id) REFERENCES "view"(view_id),
    FOREIGN KEY (column_id) REFERENCES column_def(column_id)
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
    d."name" AS dataset_name,
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
    vf.query_columns
FROM view_filter vf
    JOIN view_filter_group vfg ON vf.view_filter_group_id = vfg.view_filter_group_id
    JOIN "view" v ON vfg.view_id = v.view_id
    JOIN dataset d ON v.dataset_id = d.dataset_id
ORDER BY v.view_id, vfg.rank, vf.rank;

-- Convenience view: resolves view columns with their metadata
CREATE VIEW column_config AS
SELECT
    v.view_id,
    v.url_name AS view_url_name,
    v."name" AS view_name,
    d."name" AS dataset_name,
    vc.rank AS column_rank,
    vc.enable_by_default,
    cd."name" AS column_name,
    cd."label" AS column_label,
    cd."type" AS column_type,
    cd.sortable,
    cd.url,
    cd."delimiter"
FROM view_column vc
    JOIN "view" v ON vc.view_id = v.view_id
    JOIN dataset d ON v.dataset_id = d.dataset_id
    JOIN column_def cd ON vc.column_id = cd.column_id
ORDER BY v.view_id, vc.rank;
