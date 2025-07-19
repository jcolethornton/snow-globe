# core/connection.py
import os
from pathlib import Path
import yaml
from jinja2 import Template
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from models.args import ProfileArgs

class SnowConn:

    def __init__(self, args: ProfileArgs):
        self.args = args

    def env_var(self, name):
        """Get an environment variable with a default value."""
        return os.getenv(name, "")

    def load_profile(self):
        """Load a connection profile from a YAML file."""
        if not self.args.profile_path.exists():
            raise FileNotFoundError(f"Missing connection profile: {self.args.profile_path}")

        with open(self.args.profile_path) as f:
            raw = f.read()

        rendered = Template(raw).render(env_var=self.env_var)
        profiles = yaml.safe_load(rendered)

        if self.args.profile_name not in profiles:
            raise ValueError(f"Profile '{self.args.profile_name}' not found in {self.args.profile_path}")

        profile = profiles.get(self.args.profile_name)
        return profile

    def get_connection(self):
        """Create a Snowflake connection using a provided profile"""
        profile = self.load_profile()
        conn_params = {
            'account': profile["account_identifier"],
            'user': profile["user"],
            'role': profile["role"],
            'warehouse': profile["warehouse"],
            'database': profile["database"],
            'schema': profile["schema"]
        }

        if profile.get("private_key_path"):
            key_path = profile["private_key_path"]
            with open(key_path, "rb") as key_file:
                pkey_bytes = key_file.read()
            pkey = serialization.load_pem_private_key(
                pkey_bytes,
                password=profile.get("private_key_passphrase").encode() if profile.get("private_key_passphrase") else None,
                backend=default_backend()
            )
            pkb = pkey.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            conn_params['private_key'] = pkb

        else:
            conn_params['password'] = profile.get("password")

        return snowflake.connector.connect(**conn_params)
