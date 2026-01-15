# snow_globe/core/state.py
"""
DeployManager module for Snowflake object management.

Handles:
- Loading current state
- Generating deployment plans
- Applying deployment plans (CREATE, ALTER, DROP)
"""
import re
import json
from pathlib import Path
from sqlglot import parse_one, exp
from snow_globe.core.connection import SnowConn
from snow_globe.core.utils import hash_ddl, fetch_string
from snow_globe.core.ddl_diff import parse_columns, diff_columns, generate_alter
from snow_globe.core.lineage import LineageManager
from snow_globe.models.args import DeployArgs, TraceArgs

class DeployManager:
    """
    Manages the deployment of Snowflake objects based on SQL files and a saved state.

    Attributes:
        SAFE_RECREATE (list[str]): Object types safe to recreate entirely.
        PREFER_ALTER (list[str]): Object types that can usually be updated via ALTER statements.
        DANGEROUS_REPLACE (list[str]): Object types where replacement is risky.
        args (DeployArgs): Deployment configuration and paths.
        conn: Snowflake connection object.
        state (dict): Current state of Snowflake objects loaded from state file.
        plan (dict): Deployment plan containing new, modified, and deleted objects.
    """

    SAFE_RECREATE = [
        "view", "file format", "stage", "pipe",
        "task", "function", "procedure", "materialized view"
    ]
    PREFER_ALTER = ["table", "external table", "sequence"]
    DANGEROUS_REPLACE = ["database", "schema"]

    def __init__(self, args: DeployArgs):
        self.args = args
        self.conn = SnowConn(args).get_connection()
        self.state = None
        self.plan = {}  # initialize plan
        self.cursor = None  # initialize cursor


    def load_state(self) -> dict:
        """
        Load the saved state of Snowflake objects from the state file.

        If the state file does not exist, initializes an empty state.

        Returns:
            dict: Dictionary of objects currently tracked in the state.
        """
        if self.args.state_path.exists():
            with self.args.state_path.open() as f:
                state = json.load(f)
            self.state = state.get("objects", {})
        else:
            self.state = {"objects": {}}

    def generate_plan(self):
        """
        Generate a deployment plan by comparing SQL files against the current state.

        Determines which objects are new, modified, or deleted, and attempts to compute
        ALTER statements for modifiable objects when applicable.

        Updates self.plan with 'new_objects', 'modified_objects', and 'deleted_objects'.
        Writes the plan to self.args.plan_path as JSON.

        Returns:
            dict: The deployment plan.
        """

        self.load_state()
        current_fqns = set()
        self.plan = {"new_objects": [], "modified_objects": [], "deleted_objects": []}

        for sql_file in Path(self.args.sql_path).rglob("*.sql"):
            fqn_key, params = self.file_parameters(sql_file)
            params["ddl"] = sql_file.read_text(encoding="utf-8")
            params["hash"] = hash_ddl(params["ddl"])
            current_fqns.add(fqn_key)

            if fqn_key not in self.state:
                self.plan["new_objects"].append(params)
            else:
                current_obj = self.state[fqn_key]
                if params["hash"] != current_obj["hash"]:
                    params["current_ddl"] = current_obj["ddl"]

                    if params["object_type"] in self.PREFER_ALTER:
                        try:
                            current_cols = parse_columns(params["current_ddl"])
                            target_cols = parse_columns(params["ddl"])
                            diffs = diff_columns(current_cols, target_cols)
                            alters = generate_alter(params["fqn"], diffs)

                            alter_possible = True
                            alter_reason = None

                            # Basic safety check
                            if diffs["dropped"]:
                                alter_possible = False
                                alter_reason = "Drop column detected"
                            elif any(diffs["modified"].values()):
                                alter_possible = False
                                alter_reason = "Column type change detected"

                            params["alter_possible"] = alter_possible
                            params["alter_reason"] = alter_reason
                            params["alter_sql"] = alters

                        except Exception as e:
                            params["alter_possible"] = False
                            params["alter_reason"] = f"Failed to diff: {e}"
                            params["alter_sql"] = None

                    modified_params = self.generate_params_mod_obj(**params)
                    self.plan["modified_objects"].append(modified_params)

        # Check for deleted objects (in state but no matching file)
        for fqn_key, obj in self.state.items():
            if fqn_key not in current_fqns:
                self.plan["deleted_objects"].append(obj)

        self.validate_plan()
        with self.args.plan_path.open("w") as f:
            json.dump(self.plan, f, indent=2)

        return self.plan

    def replace_database_in_ddl(self, ddl: str, old_db: str, new_db: str) -> str:
        """
        Replace occurrences of a database name in the SQL DDL using SQLGlot AST.
        
        Args:
            ddl (str): Original SQL DDL.
            old_db (str): Current database name to replace.
            new_db (str): Target database name.

        Returns:
            str: Updated DDL with the database replaced.
        """
        tree = parse_one(ddl)

        for node in tree.find_all(exp.Table):
            if node.args.get("db") == old_db:
                node.set("db", new_db)

        return tree.sql(dialect="snowflake")

    def validate_sql(self, obj: dict):
        """
        Validate a SQL statement using Snowflake EXPLAIN to check syntax and feasibility.

        Args:
            obj (dict): Object containing DDL and metadata ('ddl', 'database', 'schema').

        Returns:
            tuple[str, str | None, str | None]:
                - result: "OK" or "ERROR"
                - error_code: Snowflake error code if validation fails, otherwise 0
                - message: Detailed error or success message
        """

        ddl = obj['ddl']
        database = obj['database']
        if self.args.environment != "prod":
            if self.args.database_prefix:
                database = f"{self.args.database_prefix}{obj['database']}"
                ddl = self.replace_database_in_ddl(ddl, obj["database"], database)

        self.cursor.execute(f"USE SCHEMA {database}.{obj['schema']}")
        query = f"EXPLAIN USING JSON {ddl}"
        try:
            results = fetch_string(self.cursor, query)
            if "GlobalStats" in results:
                return "OK", 0, None
        except Exception as e:
            error_code = str(e).split(' ', maxsplit=1)[0]
            return "ERROR", error_code, f"{e}"


    def _validate_new_objects(self):
        """Validate new objects for SQL correctness and dependency handling."""
        for obj in self.plan.get("new_objects", []):
            file_path = self.args.sql_path / Path(obj['file_path'])
            ddl = file_path.read_text(encoding="utf-8")

            if self.args.environment != "prod" and self.args.database_prefix:
                database = f"{self.args.database_prefix}{obj['database']}"
                fqn = obj['fqn'].replace(obj['database'], database)
            else:
                database = obj['database']
                fqn = obj['fqn']

            self.cursor.execute(f"USE SCHEMA {database}.{obj['schema']}")
            result, error_code, msg = self.validate_sql(obj)

            dep_object = None
            if error_code == '002003':
                match = re.search(r"'([^']+)'", msg)
                if match:
                    dep_object = match.group(1).lower()

                if dep_object:
                    for new_obj in (o for o in self.plan["new_objects"] if o != obj):
                        if dep_object == new_obj['name'].lower():
                            msg = f"Dependent on {obj['fqn']}"
                            result = "OK"

            obj['validation'] = result
            obj['message'] = msg

    def _validate_modified_objects(self):
        """Validate modified objects for SQL correctness and ALTER feasibility."""
        for obj in self.plan.get("modified_objects", []):
            result, error_code, msg = self.validate_sql(obj)

            if result == "OK" and obj["object_type"] in self.PREFER_ALTER:
                if obj.get("alter_possible"):
                    result = "OK: ALTER"
                else:
                    result = "WARNING: REFRESH"
                    msg = f"refresh required: {obj.get('alter_reason')}"

            obj['validation'] = result
            obj['message'] = msg

    def _validate_deleted_objects(self):
        """Validate deleted objects for references in other objects (lineage check)."""
        for obj in self.plan.get("deleted_objects", []):
            trace_args = TraceArgs(
                state=self.state,
                fqn=obj['fqn'],
                object_type=obj['type'],
                verbose=False,
                quiet=True,
            )
            trace = LineageManager(trace_args)
            trace.trace_object_lineage()
            children = trace.get_children()

            result = "OK"
            msg = None
            if children:
                state_key = f"{obj['type']}-{obj['fqn']}".strip().lower()
                if state_key not in children:
                    result = "WARNING object reference found:"
                    msg = children

            obj['validation'] = result
            obj['message'] = msg

    def validate_plan(self):
        """
        Validate all objects in the deployment plan.

        Updates each object with 'validation' and 'message' fields.
        """
        self.cursor = self.conn.cursor()

        self._validate_modified_objects()
        self._validate_new_objects()
        self._validate_deleted_objects()


    def apply_plan(self, mode="alter_first", dry_run=False):
        """
        Apply the deployment plan by executing SQL statements in the target Snowflake environment.

        Args:
            mode (str, optional): "alter_first" attempts ALTER statements before CREATE OR REPLACE.
            dry_run (bool, optional): If True, SQL statements are printed but not executed.

        Executes DDL statements on Snowflake unless dry_run=True.
        """
        with self.args.plan_path.open() as f:
            plan = json.load(f)

        for obj in plan["modified_objects"]:
            db = obj["database"]
            fqn = obj["fqn"]
            file_path = Path(obj["file_path"])
            with file_path.open('r', encoding='utf-8') as f:
                ddl = f.read()

            if self.args.environment != "prod":
                if self.args.database_prefix:
                    db = f"{self.args.database_prefix}{db}"
                fqn = fqn.replace(obj["database"], db)

            current_ddl = obj.get("ddl")  # from state file
            if not current_ddl:
                # No previous DDL, fallback
                self.run_sql(ddl, fqn, dry_run)
                continue

            # Check if ALTER is possible
            try:
                current_cols = parse_columns(current_ddl)
                target_cols = parse_columns(ddl)
                diffs = diff_columns(current_cols, target_cols)
                alters = generate_alter(fqn, diffs)

                if mode == "alter_first" and alters:
                    for alter in alters:
                        self.run_sql(alter, fqn, dry_run)
                else:
                    self.run_sql(ddl, fqn, dry_run)

            except Exception as e:
                if not self.args.quiet:
                    print(f"⚠️ Fallback to CREATE OR REPLACE for {fqn}: {e}")
            break

    def deploy_ddl(self):
        """
        Execute SQL statements within a Snowflake transaction.

        Ensures atomic execution with COMMIT or ROLLBACK in case of errors.

        Executes SQL statements from a file in the target database.
        """

        with file_path.open('r', encoding='utf-8') as f:
            content = f.read()
        try:
            cursor.execute("BEGIN")
            self.connection.execute_string(content)
            cursor.execute("COMMIT")
            if not self.args.quiet:
                print("Transaction committed successfully.")
        except Exception as e:
            cursor.execute("ROLLBACK")
            if not self.args.quiet:
                print(f"Error executing statement: {e}")
                print("Transaction rolled back.")
        finally:
            cursor.close()

    def generate_params_mod_obj(self, **kwargs) -> dict:
        """
        Construct a standardized dictionary for a modified object.

        Args:
            **kwargs: Fields like database, schema, object_type, name, ddl, alter_possible, etc.

        Returns:
            dict: Standardized object dictionary used in self.plan.
        """
        return {
            "database": kwargs["database"].lower(),
            "schema": kwargs["schema"].lower(),
            "object_type": kwargs["object_type"].lower(),
            "name": kwargs["name"].replace('.sql','').strip().lower(),
            "fqn": kwargs["fqn"],
            "ddl": kwargs["ddl"],
            "alter_possible": kwargs.get("alter_possible"),
            "alter_sql": kwargs.get("alter_sql",[]),
            "alter_reason": kwargs.get("alter_reason"),
            "current_ddl": kwargs.get("current_ddl"),
            "file_path": kwargs["file_path"]
        }

    def file_parameters(self, sql_file) -> tuple[str, dict]:
        """
        Extract metadata and fully qualified name (FQN) from a SQL file path.

        Args:
            sql_file (Path): Path to the SQL file.

        Returns:
            tuple[str, dict]:
                - key: Unique key combining object type and FQN (e.g., 'table-mydb.myschema.mytable')
                - params: Dictionary containing database, schema, object_type, name, FQN, and relative file path.

        Raises:
            ValueError: If the SQL file path does not match the expected folder structure.
        """
        relative = sql_file.relative_to(self.args.sql_path)
        parts = relative.parts
        if len(parts) < 7:
            raise ValueError(f"Unexpected file path structure: {sql_file}")
        params = {
            "database": parts[2].lower(),
            "schema": parts[4].lower(),
            "object_type": parts[5].lower(),
            "name": parts[6].replace('.sql','').strip().lower(),
            "fqn": f"{parts[2]}.{parts[4]}.{parts[6].replace('.sql','')}".lower(),
            "file_path": str(relative)
        }
        return f"{params['object_type']}-{params['fqn']}".strip().lower(), params
