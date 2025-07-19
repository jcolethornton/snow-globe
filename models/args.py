# models/args.py
from typing import Type
from pydantic import BaseModel, ConfigDict
from models.mixins import (
    OutputMixin,
    StatePathMixin,
    ProfileMixin,
    DeployMixin,
    StateMixin,
    TraceMixin
    )

class Base(BaseModel):
    # To forbid unexpected args (optional)
    model_config = ConfigDict(extra="ignore")

class RefreshArgs(Base, StatePathMixin, OutputMixin, ProfileMixin):
    pass

class ProfileArgs(Base, OutputMixin, ProfileMixin):
    pass

class DeployArgs(Base, DeployMixin):
    pass

class StateArgs(Base, StateMixin):
    pass

class TraceArgs(Base, TraceMixin):
    pass
