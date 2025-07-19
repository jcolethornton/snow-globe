# models/config.py
import yaml
from typing import Type
from pathlib import Path
from pydantic import BaseModel

def merge_config(cli: dict, yaml_config: dict, model: Type[BaseModel]) -> BaseModel:
    """Merge CLI arguments with YAML configuration and validate against a Pydantic model."""
    # Merge YAML and CLI, CLI overrides YAML
    merged = {**yaml_config, **cli}
    # Remove keys explicitly set to None so Pydantic uses defaults
    cleaned = {k: v for k, v in merged.items() if v is not None}
    return model(**cleaned)

def parse_config(
    profile: str,
    model_cls: Type[BaseModel],
    environment: str = "prod",
    profiles_path: Path = Path("profiles.yml"),
    config_path: Path = Path("config.yml")
) -> dict:
    """
    Combine profiles.yml and config.yml, applying profile and environment overrides.
    """
    # Load YAML files
    with profiles_path.open("r") as f:
        profiles_file = yaml.safe_load(f)

    with config_path.open("r") as f:
        config_file = yaml.safe_load(f)

    # Load base configs
    profile_config = profiles_file.get(profile, {})
    tool_config = config_file.get(profile, {})

    # Load environment overrides if present
    environments = tool_config.get("environments", {})
    env_config = environments.get(environment, {})
    print(env_config)

    # Start with model defaults
    defaults = {field: getattr(model_cls(), field) for field in model_cls.model_fields}

    # Merge: defaults -> profile_config -> tool_config -> env_config
    merged_config = defaults.copy()
    merged_config.update(profile_config)
    merged_config.update(tool_config)
    merged_config.update(env_config)

    # Special case: set environment & database_prefix in merged_config
    merged_config["environment"] = environment
    merged_config["database_prefix"] = env_config.get("database_prefix", "")

    return merged_config
