# models/args.py
from typing import Type
from pydantic import BaseModel, ConfigDict
from snow_globe.models.mixins import (
    OutputMixin,
    StatePathMixin,
    DeployMixin,
    StateMixin,
    TraceMixin
    )

class Base(BaseModel):
    # To forbid unexpected args (optional)
    model_config = ConfigDict(extra="ignore")

class RefreshArgs(Base, StatePathMixin, OutputMixin):
    pass

class DeployArgs(Base, DeployMixin):
    pass

class StateArgs(Base, StateMixin):
    pass

class TraceArgs(Base, TraceMixin):
    pass
