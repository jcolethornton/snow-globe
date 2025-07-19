# snow_globe/core/state.py
import re
import json
from pathlib import Path
from core.connection import SnowConn
from core.utils import hash_ddl, fetch_string
from models.args import DeployArgs, TraceArgs
from core.ddl_diff import parse_columns, diff_columns, generate_alter
from core.lineage import LineageManager

class DeployManager:
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


    def load_state(self) -> dict:
        """Load state file"""
        if self.args.state_path.exists():
            with self.args.state_path.open() as f:
                state = json.load(f)
            self.state = state["objects"]
        else:
            self.state = {"objects": {}}

    def generate_plan(self):
        """Generate the plan"""

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

                            # Basic safety check (you can make this smarter)
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

    def validate_sql(self, obj: dict):
        """Validate SQL using Snowflake Explain"""

        ddl = obj['ddl']
        database = obj['database']
        if self.args.environment != "prod":
            if self.args.database_prefix:
                database = f"{self.args.database_prefix}{obj['database']}"
                ddl = ddl.replace(obj["database"], database)

        self.cursor.execute(f"USE SCHEMA {database}.{obj['schema']}")
        query = f"EXPLAIN USING JSON {ddl}"
        try:
            results = fetch_string(self.cursor, query)
            if "GlobalStats" in results:
                return "OK", 0, None
        except Exception as e:
            error_code = str(e).split(' ')[0]
            return "ERROR", error_code, f"{e}"


    def validate_plan(self):
        """Validate the plan"""


        self.cursor = self.conn.cursor()

        for category in self.plan:

            if category == "modified_objects":
                for obj in self.plan[category]:
                    result, error_code, msg = self.validate_sql(obj)
                    if result == "OK":
                        if obj["object_type"] in self.PREFER_ALTER:
                            if obj.get('alter_possible'):
                                result = "OK: ALTER"
                            else:
                                result = "WARNING: REFRESH"
                                msg = f"refresh required: {obj['alter_reason']}"

                    obj['validation'] = result
                    obj['message'] = msg

            elif category == "new_objects":
                for obj in self.plan[category]:
                    file_path = self.args.sql_path / Path(obj['file_path'])
                    with file_path.open() as f:
                        ddl = f.read()
                    self.cursor.execute(f"USE SCHEMA DEV_{obj['database']}.{obj['schema']}")
                    result, error_code, msg = self.validate_sql(obj)

                    if error_code == '002003':
                        match = re.search(r"'([^']+)'", msg)
                        if match:
                            dep_object = match.group(1).lower()

                        for new_obj in (o for o in self.plan["new_objects"] if o != obj):
                            if dep_object == new_obj['name'].lower():
                                msg = f"Dependent on {obj['fqn']}"
                                result = "OK"

                    obj['validation'] = result
                    obj['message'] = msg
            elif category == "deleted_objects":
                for obj in self.plan[category]:
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
                        warn_msg = []
                        state_key = f"{obj['type']}-{obj['fqn']}".strip().lower()
                        if state_key not in children:
                            warn_msg.append(state_key)
                            result = "WARNING object reference found:"
                        if warn_msg:
                            msg = children
                    obj['validation'] = result
                    obj['message'] = msg


    def adjust_fqn(self, ddl: str) -> str:
        if self.args.environment != "prod":
            # Rewrite prod DB names to dev equivalents
            prod_db = self.args.original_db
            dev_db = f"{self.args.database_prefix}{prod_db}"
            return ddl.replace(prod_db, dev_db)
        return ddl

    # def apply_env_to_fqn(self, fqn: str) -> str:
    #     """
    #     Adjust the fully qualified name (FQN) for the current environment.
    #     """
    #     prefix = self.args.database_prefix or ""
    #     parts = fqn.split(".")
    #     if len(parts) == 3:
    #         db, schema, obj = parts
    #         return f"{prefix}{db}.{schema}.{obj}"
    #     return fqn  # If FQN not fully qualified

    def apply_plan(self, mode="alter_first", dry_run=False):
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
        Executes SQL statmemnts in a Transaction.
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

    # def state_change(self, object_name: str, object_type: str, state_path: Path) -> bool:
    #     """
    #     Check if the file hash for a given object matches the stored hash in the state.
    #     Returns True if changed, False if unchanged, raises KeyError/FileNotFoundError if not found.
    #     """
    #     state_key = f"{object_type.lower()}-{object_name.lower()}"
    #     obj = self.state.get(state_key)
    #     if obj is None:
    #         raise KeyError(f"Object {state_key} not found in state file.")
    #     file_path = Path(obj["file_path"])
    #     if not file_path.exists():
    #         raise FileNotFoundError(f"File {file_path} not found.")
    #     with file_path.open(encoding="utf-8") as f:
    #         contents = f.read()
    #     file_hash = hash_ddl(contents)
    #     return file_hash != obj["hash"]
