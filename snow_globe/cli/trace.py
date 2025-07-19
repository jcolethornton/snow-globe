# cli/trace.py
import typer
from pathlib import Path
from snow_globe.models.args import TraceArgs
from snow_globe.core.lineage import LineageManager

trace_cmd = typer.Typer()

def default(model, field):
    return model.model_fields[field].default

@trace_cmd.command("run")
def lineage(
    fqn: str = typer.Option(None, help="object fqn to trace lineage"),
    object_type: str = typer.Option(None, help="object type to trace lineage"),
    state_path: Path = typer.Option(default(TraceArgs, 'state_path'), help="Path to state file"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    quiet: bool = typer.Option(False, help="Suppress output"),
):
    cli_args = TraceArgs(
        state_path= state_path,
        fqn=fqn,
        object_type=object_type,
        verbose= verbose,
        quiet=quiet,
    )

    lineage = LineageManager(cli_args)
    lineage.load_state()
    lineage.trace_object_lineage()
