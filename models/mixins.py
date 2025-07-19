# models/mixins.py
import typer
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field
from typing_extensions import Annotated


class OutputMixin(BaseModel):
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging")] = Field(
        default=False,
        description="Enable verbose logging"
    )
    quiet: Annotated[bool, typer.Option(help="Suppress output")] = Field(
        default=False,
        description="Suppress output"
    )

class StatePathMixin(BaseModel):
    state_path: Annotated[Path, typer.Option(help="Path to state file")] = Field(
        default=Path("data/state.json"),
        description="Path to state file"
    )
    state: Optional[Annotated[dict, typer.Option(help="State object")]] = Field(
        default=None,
        description="state object"
    )

class ProfileMixin(BaseModel):
    profile_path: Annotated[Path, typer.Option(help="Path to the profile file")] = Field(
        default=Path("./profiles.yml"),
        description="Path to the profile file"
    )
    profile_name: Annotated[str, typer.Option(help="Profile name")] = Field(
        default="default",
        description="Profile name"
    )

class DeployMixin(StatePathMixin, OutputMixin, ProfileMixin):
    sql_path: Annotated[Path, typer.Option(help="Path to SQL files")] = Field(
        default=Path("ddl_management"),
        description="Path to SQL files"
    )
    plan_path: Annotated[Path, typer.Option(help="Path to plan file")] = Field(
        default=Path("data/plan.json"),
        description="Path to plan file"
    )
    environment: str = Field(
        default="prod",
        description="Environment for the connection (e.g., 'prod', 'dev')"
    )
    database_prefix: str = Field(
        default="",
        description="database prefix"
    )

class StateMixin(StatePathMixin, OutputMixin, ProfileMixin):
    account_identifier: Optional[str] = Field(
        default=None,
        description="Account identifier for Snowflake connection"
    )
    database_schema: Optional[List[str]] = Field(
        default_factory=lambda: ["SNOWFLAKE"],
        description="List of database schemas to manage"
    )
    object_types: Optional[List[str]] = Field(
        default_factory=lambda: ["TABLE","VIEW"],
        description="List of object types to manage"
    )
    threads: int = Field(
        default=10,
        description="Number of concurrent threads"
    )

class TraceMixin(StatePathMixin, OutputMixin):
    fqn: str = Field(
        default=None,
        description="Object fqn to trace"
    )
    object_type: str = Field(
        default=None,
        description="Object type to trace"
    )
