# core/queries.py
class Queries:

    SHOW_OBJECT_CMD = """
    show {object_type}s in {location}
    ->> 
    select "database_name", "schema_name", "name"
    from $1 where "schema_name" <> 'INFORMATION_SCHEMA'
    """

    SHOW_PROC_CMD = """
    show {object_type}s in {location}
    ->> 
    select "catalog_name" as "database_name", "schema_name", split_part("arguments", 'RETURN', 1) as "name", split_part("arguments", '(', 1) as "clean_name"
    from $1 where "catalog_name" <> '' and "is_builtin" = 'N'
    """

    SHOW_STAGE_CMD = """
    show stages in {location}
    ->> 
    select "database_name", "schema_name", "name", "url", "storage_integration", "comment", "directory_enabled"
    from $1
    """
