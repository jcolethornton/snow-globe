# snow_globe/core/lineage.py
import re
import json
from pathlib import Path
from models.args import TraceArgs

class LineageManager:

    def __init__(self, args: TraceArgs):
        self.state = getattr(args, 'state', None)
        self.args = args
        self.children: List[str] = []

    def load_state(self) -> dict:
        """Load state file"""
        if self.args.state_path.exists():
            with self.args.state_path.open() as f:
                state = json.load(f)
            self.state = state["objects"]
        else:
            self.state = {"objects": {}}

    def trace_object_lineage(self):
        """Trace and collect the lineage of an object"""
        if self.state is None:
            raise ValueError("State has not been loaded. Call load_state() first.")

        state_key = f"{self.args.object_type}-{self.args.fqn}".strip().lower()
        if state_key not in self.state:
            print(f"{fqn} not in state")
            return
        def _trace(fqn, object_type, depth=0):
            if not self.args.quiet:
                print("  " * depth + f"{object_type}: {fqn}")
            state_key = f"{object_type}-{fqn}".strip().lower()
            self.children.append(state_key)
            parent_obj = self.state[state_key]
            for k, child_obj in self.state.items():
                if k not in self.children:
                    if parent_obj['fqn'] in child_obj["ddl"].lower():
                        self.children.append(k)

                        _trace(child_obj['fqn'], child_obj['type'], depth + 1)
                    elif child_obj['database'] == parent_obj['database'] and child_obj['schema'] == parent_obj['schema']:
                        if f"{parent_obj['name']} " in child_obj['ddl'].lower():
                            self.children.append(k)
                            _trace(child_obj['fqn'], child_obj['type'], depth + 1)

        if not self.args.quiet:
            print(f"📈 Lineage for {self.args.object_type}:{self.args.fqn}")
        _trace(self.args.fqn, self.args.object_type)
        state_key = f"{self.args.object_type}-{self.args.fqn}".strip().lower()
        self.children.remove(state_key)


    def get_children(self):
        return self.children
