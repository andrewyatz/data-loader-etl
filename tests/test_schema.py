def test_sql_schema_loads(db_connection, sql_dir, sql_version):
    path = sql_dir / f"schema.{sql_version}.sql"
    with open(path, "rt") as fh:
        content = fh.read()
        db_connection.execute(content)
