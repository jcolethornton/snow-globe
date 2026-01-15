# cli/state.py
import typer
from pathlib import Path
from snow_globe.models.args import StateArgs, DeployArgs
from snow_globe.models.config import parse_config, merge_config
from snow_globe.core.state import StateManager
from snow_globe.core.deploy import DeployManager
from snow_globe.core.outputs import print_plan

state_cmd = typer.Typer()

def default(model, field):
    return model.model_fields[field].default

@state_cmd.command("refresh")
def refresh(
    state_path: Path = typer.Option(default(StateArgs, 'state_path'), help="Path to state file"),
    threads: int = typer.Option(None, help="Number of concurrent threads"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    quiet: bool = typer.Option(False, help="Suppress output"),
):
    cli_args = {
        "state_path": state_path,
        "threads": threads,
        "verbose": verbose,
        "quiet": quiet,
    }

    config = parse_config(StateArgs)
    args = merge_config(cli_args, config, StateArgs)

    state = StateManager(args)
    state.refresh_state()


@state_cmd.command("plan")
def plan(
    environment: str = typer.Option(default(DeployArgs, 'environment'), help="Environment [prod|dev]"),
    state_path: Path = typer.Option(default(DeployArgs, 'state_path'), help="Path to state file"),
    sql_path: Path = typer.Option(None, help="Path to SQL files"),
    plan_path: Path = typer.Option(None, help="Path to plan file"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    quiet: bool = typer.Option(False, help="Suppress output"),
):
    cli_args = {
        "environment": environment,
        "state_path": state_path,
        "sql_path": sql_path,
        "plan_path": plan_path,
        "verbose": verbose,
        "quiet": quiet,
    }

    config = parse_config(DeployArgs, environment)
    args = merge_config(cli_args, config, DeployArgs)

    deploy = DeployManager(args)
    plan_result = deploy.generate_plan()
    print_plan(plan_result)

@state_cmd.command("apply")
def apply(
    environment: str = typer.Option(default(DeployArgs, 'environment'), help="Environment [prod|dev]"),
    state_path: Path = typer.Option(default(DeployArgs, 'state_path'), help="Path to state file"),
    sql_path: Path = typer.Option(None, help="Path to SQL files"),
    plan_path: Path = typer.Option(None, help="Path to plan file"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    quiet: bool = typer.Option(False, help="Suppress output"),
):
    cli_args = {
        "environment": environment,
        "state_path": state_path,
        "sql_path": sql_path,
        "plan_path": plan_path,
        "verbose": verbose,
        "quiet": quiet,
    }

    config = parse_config(DeployArgs, environment)
    args = merge_config(cli_args, config, DeployArgs)

    deploy = DeployManager(args)
    deploy.apply_plan()
