import sqlglot
from sqlglot import parse_one, exp


def parse_columns(ddl: str) -> dict:
    """
    Parse a CREATE TABLE DDL and return a dict of columns and types.
    """
    tree = parse_one(ddl)
    columns = {}
    for col in tree.find_all(exp.ColumnDef):
        col_name = col.name.lower()
        col_type = col.args["kind"].sql().lower()
        columns[col_name] = col_type
    return columns


def diff_columns(current: dict, target: dict) -> dict:
    """
    Compare current and target column definitions.
    Return a dict with added, dropped, and modified columns.
    """
    current_keys = set(current)
    target_keys = set(target)

    added = {k: target[k] for k in target_keys - current_keys}
    dropped = {k: current[k] for k in current_keys - target_keys}
    modified = {
        k: {"from": current[k], "to": target[k]}
        for k in current_keys & target_keys
        if current[k] != target[k]
    }

    return {"added": added, "dropped": dropped, "modified": modified}


def generate_alter(table_name: str, diff: dict) -> list[str]:
    """
    Generate ALTER TABLE statements from diff.
    """
    alters = []

    for col, col_type in diff["added"].items():
        alters.append(f'ALTER TABLE {table_name} ADD COLUMN {col} {col_type};')

    for col in diff["dropped"]:
        alters.append(f'ALTER TABLE {table_name} DROP COLUMN {col};')

    for col, change in diff["modified"].items():
        # NOTE: Some types can't be changed in Snowflake directly
        alters.append(
            f'-- Column {col} type change from {change["from"]} to {change["to"]}'
        )
        alters.append(f'ALTER TABLE {table_name} ALTER COLUMN {col} SET DATA TYPE {change["to"]};')

    return alters
