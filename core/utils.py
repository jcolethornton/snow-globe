# core/utils.py
def hash_ddl(sql: str) -> str:
    """
    sha256 hash of the DDL statement, normalized and cleaned.
    """
    import re
    import hashlib
    # Normalize line endings
    normalized = sql.replace('\r\n', '\n').rstrip()
    # Remove non-printable characters (ASCII 0–31, 127)
    cleaned = re.sub(r'[^\x20-\x7E]', '', normalized)
    return hashlib.sha256(cleaned.encode('utf-8')).hexdigest()


def fetch_df(cursor, query_template: str, **params):
    """
    Run a parameterized SQL query and return a DataFrame.

    Parameters:
    - query_template: SQL string with placeholders, e.g. {days}, {user}
    - params: key-value arguments to format into the query

    Returns:
    - pd.DataFrame: query results with proper column names
    """
    import pandas as pd
    try:
        query = query_template.format(**params)
    except KeyError as e:
        raise ValueError(f"Missing parameter for query: {e}")

    try:
        cursor.execute(query)
    except Exception as e:
        raise RuntimeError(f"Failed to execute query: {query}\nError: {e}")

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=columns)


def fetch_string(cursor, query):
    """Run a SQL query and return a single string result"""
    cursor.execute(query)
    return cursor.fetchone()[0]


def execute_transaction_from_file(cursor, file_path):
    """
    Executes SQL statmemnts in a Transaction.
    """
    from pathlib import Path
    with file_path.open('r', encoding='utf-8') as f:
        content = f.read()
    try:
        cursor.execute("BEGIN")
        self.connection.execute_string(content)
        cursor.execute("COMMIT")
        print("Transaction committed successfully.")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Error executing statement: {e}")
        print("Transaction rolled back.")
    finally:
        cursor.close()

def deploy_ddl(cursor, state_object: dict = None):
    """
    Executes SQL statmemnts in a Transaction.
    """
    # db = state_object.get("database", None)
    # schema = state_object.get("schema", None)
    # stmt = f"{"USE DATABASE" db if db else None}"
    # stmt = f"{"USE SCHEMA" db if db else None}"

    with file_path.open('r', encoding='utf-8') as f:
        content = f.read()
    try:
        cursor.execute("BEGIN")
        self.connection.execute_string(content)
        cursor.execute("COMMIT")
        print("Transaction committed successfully.")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Error executing statement: {e}")
        print("Transaction rolled back.")
    finally:
        cursor.close()


def set_env():
    default = "dev_"
