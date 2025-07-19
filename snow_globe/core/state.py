# core/state.py
import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from typer import style, echo
from typer.colors import GREEN, RED, YELLOW
from snow_globe.core.queries import Queries
from snow_globe.core.connection import SnowConn
from snow_globe.core.utils import hash_ddl, fetch_df, fetch_string
from snow_globe.models.args import StateArgs

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class StateManager:
    def __init__(self, args: StateArgs):
        self.args = args
        self.objects = {}
        self.query = None
        self.conn = SnowConn(args).get_connection()
        self.lock = Lock()  # for thread-safe access to self.objects

    def refresh_state(self):
        self.cursor = self.conn.cursor()
        try:
            for obj in self.args.object_types:
                self.object_type = obj
                self.query = self.queries(obj)

                if self.args.database_schema:
                    for database in self.args.database_schema:
                        location = f"database {database}"
                        self.export_state(loc=location)
                else:
                    location = "account"
                    self.export_state(loc=location)

            if not self.args.quiet:
                echo(style("-" * 60, fg=YELLOW))
                echo(style("EXPORTED TO STATE:", fg=GREEN))
                for obj_type in self.args.object_types:
                    obj_count = sum(1 for obj in self.objects.values() if obj["type"] == obj_type)
                    echo(f"• {obj_count} {obj_type}{'s' if obj_count > 1 else ''}.")

            self.state = {"objects": self.objects}
            self.save_state()
        finally:
            self.cursor.close()
            self.conn.close()

    def queries(self, obj):
        if obj in ["procedure", "function"]:
            return Queries.SHOW_PROC_CMD
        elif obj == "stage":
            return Queries.SHOW_STAGE_CMD
        return Queries.SHOW_OBJECT_CMD

    def get_ddl(self, object_path, **kwargs):
        """Fetch DDL and update self.objects"""
        if self.object_type == "stage":
            ddl = f"""CREATE OR REPLACE STAGE {kwargs['fqn']}
            URL='{kwargs.get('url')}'
            STORAGE_INTEGRATION={kwargs.get('storage_integration')}
            {'DIRECTORY=(ENABLE=TRUE)' if kwargs.get('directory_enabled') else ''};
            """
        else:
            obj_type = self.object_type.replace(' ', '_')
            ddl = fetch_string(
                self.conn.cursor(),  # New cursor per thread
                f"select get_ddl('{obj_type}','{kwargs['fqn']}')"
            )

        state_key = f"{self.object_type}-{kwargs['clean_fqn']}".strip().lower()
        with self.lock:
            self.objects[state_key] = {
                "name": kwargs.get('name').strip().lower(),
                "database": kwargs.get('database_name').strip().lower(),
                "schema": kwargs.get('schema_name').strip().lower(),
                "fqn": kwargs['fqn'].strip().lower(),
                "type": self.object_type,
                "ddl": ddl,
                "hash": hash_ddl(ddl),
                "file_path": str(object_path),
                "last_seen": datetime.utcnow().isoformat()
            }

        self.save_sql(object_path, ddl)
        return kwargs['fqn']

    def export_state(self, loc):
        df = fetch_df(
            self.cursor,
            self.query,
            location=loc,
            object_type=self.object_type
        )

        row_count = df.shape[0]
        log.debug(f"Fetched {row_count} rows from: {self.query}")

        def process_row(index, row):
            params = dict(row._asdict())
            params['fqn'] = f"{params.get('database_name')}.{params.get('schema_name')}.{params.get('name')}"
            params['clean_fqn'] = f"{params.get('database_name')}.{params.get('schema_name')}.{params.get('clean_name', params.get('name'))}"
            self.schema_path = Path('ddl_management') /\
                Path(self.args.account_identifier) /\
                Path("databases") /\
                Path("schemas") /\
                Path(params.get("schema_name").lower()) /\
                Path(self.object_type.lower())
            object_path = Path(self.schema_path) / f"{params.get('clean_name', params.get('name')).lower()}.sql"

            if not self.args.quiet:
                echo(
                    style("Queued for export: ", bold=True)
                    + style(f"{index}", fg=YELLOW) + " of " + style(f"{row_count}", fg=YELLOW)
                    + f" {self.object_type}{'s' if row_count > 1 else ''} - "
                    + style(f"{params['clean_fqn']}", fg=GREEN)
                )

            return self.get_ddl(object_path, **params)

        # Run in parallel
        with ThreadPoolExecutor(max_workers=self.args.threads) as executor:
            futures = {
                executor.submit(process_row, idx, row): idx
                for idx, row in enumerate(df.itertuples(index=False), start=1)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    fqn = future.result()
                    if not self.args.quiet:
                        echo(style(f"Completed export for {fqn}", fg=GREEN))
                except Exception as e:
                    echo(style(f"Error exporting {idx}: {e}", fg=RED))

    def save_sql(self, object_path, ddl):
        self.schema_path.mkdir(parents=True, exist_ok=True)
        with open(object_path, 'w') as f:
            f.write(ddl)

    def save_state(self):
        with self.args.state_path.open("w") as f:
            json.dump(self.state, f, indent=2)
