# ❄️ Snow Globe

A lightweight CLI tool for managing Snowflake infrastructure and extracting DDLs.  
Inspired by the simplicity of `dbt`, Snow Globe lets you export, trace, and manage Snowflake objects directly from your terminal.

---

## Features

- ⚡ Export full schema DDLs with a single command
- 🔗 Trace object lineage across databases and schemas
- 📂 Save DDLs to disk in organized folders

---

## 📦 Installation

### Using pip
```bash
pip install snow-globe
```

### From source
Clone the repository and install:

```bash
git clone https://github.com/yourusername/snow-globe.git
cd snow-globe
pip install .
```

## 🛠️ Requirements
Python 3.11+
A Snowflake account
Snowflake credentials set as environment variables or in a config file

## ⚡Quick Start
Export all current snowflake objects to state
```bash
snow-globe state refresh \
    --object-types table view \
    --account-identifier xy12345 \
    --state-path ./state.json
```
This will:
- Connect to your Snowflake account
- Fetch DDLs for all tables and views
- Save them under ddl_management/ and state.json

## Trace object lineage
```bash
snow-globe trace run \
    --database my_database \
    --schema my_schema \
    --object my_table
```
See which objects depend on my_table across your Snowflake account


## 🔑 Configuration
By default, Snow Globe uses Snowflake credentials from environment variables:

Variable	Example
SNOWFLAKE_USER	jdoe
SNOWFLAKE_PASSWORD	supersecret
SNOWFLAKE_ACCOUNT	xy12345.region.aws
SNOWFLAKE_WAREHOUSE	COMPUTE_WH
SNOWFLAKE_ROLE	SYSADMIN


## Roadmap
- Config file support (~/.snow_globe/config.yml)
- Dry run mode
- Support for Snowflake Databases, schemas and warehouses
- Git integration for DDL change tracking

## 🤝 Contributing
Pull requests and issues are welcome! See CONTRIBUTING.md for guidelines.

## 📝 License
MIT © 2025 Jaryd Thornton
