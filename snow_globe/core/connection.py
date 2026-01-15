# core/connection.py
import os
from pathlib import Path
import yaml
from jinja2 import Template
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from snow_globe.models.args import DeployArgs

class SnowConn:

    def __init__(self, args: DeployArgs):
        self.args = args

    def env_var(self, name):
        """Get an environment variable with a default value."""
        return os.getenv(name, "")

    def load_config(self):
        """Load a connection profile from config"""
        if not self.args.config_path.exists():
            raise FileNotFoundError(f"Missing config: {self.args.config_path}")

        with open(self.args.config_path) as f:
            raw = f.read()

        rendered = Template(raw).render(env_var=self.env_var)
        return yaml.safe_load(rendered)

    def get_connection(self):
        """Create a Snowflake connection"""
        config = self.load_config()
        if self.args.environment not in config['environments']:
            raise Exception(f"Environment '{self.args.environment}' not found in {self.args.config_path}")

        account = config['environments'][self.args.environment]['account_identifier']
        conn_params = {
            'account': account,
            'user': config["user"],
            'role': config["role"],
            'warehouse': config["warehouse"],
            'database': self.args.default_database,
            'schema': self.args.default_schema
        }
        key_path = config["private_key_path"]
        with open(key_path, "rb") as key_file:
            pkey_bytes = key_file.read()
        pkey = serialization.load_pem_private_key(
            pkey_bytes,
            password=config.get("private_key_passphrase").encode() if config.get("private_key_passphrase") else None,
            backend=default_backend()
        )
        pkb = pkey.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        conn_params['private_key'] = pkb

        return snowflake.connector.connect(**conn_params)
