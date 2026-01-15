# cli/init.py
import shutil
import typer
from jinja2 import Environment, PackageLoader
from pathlib import Path
from rich import print
from rich.prompt import Prompt, IntPrompt, InvalidResponse
import importlib.resources as resources

setup_cmd = typer.Typer()

@setup_cmd.command("init")
def init(
    dest_dir: Path = typer.Option(
        Path.cwd(), "--path", "-p", help="Directory to initialize the project"
    )
):
    """
    Initialize Snow-Globe in a directory by creating profiles.yml and scaffold folders.
    """
    print(f"[bold]Welcome to Snow-Globe![/bold]")

    # Directories to create
    dirs_to_create = ["ddl_management", "logs", "data"]
    for folder in dirs_to_create:
        folder_path = dest_dir / folder
        folder_path.mkdir(exist_ok=True)
        print(f"[green]✔ Created {folder}/ folder[/green]")

    # templates
    template_env = Environment(
        loader=PackageLoader("snow_globe", "templates"),
        trim_blocks=True,
        lstrip_blocks=True
    )

    print("Snow-Globe is designed to help manage your Snowflake environments")
    print("Lets begin by configuring your setup.")

    # objects
    schema_objects = [
        "table",
        "view",
        "stream",
        "stage",
        "sequence",
        "procedure",
        "function",
        "task",
        "pipe",
        "file format",
    ]
    manahged_schema_objects = []
    manage_all_schema_objects = Prompt.ask(f"Do you want Snow-Globe to manage all schema level objects? [Y/n] ", default="Y")
    if manage_all_schema_objects.lower() in ['n', 'no']:
        for obj_type in schema_objects:
            manage = Prompt.ask(f"Do you want Snow-Globe to manage {obj_type}? [Y/n] ", default="Y")
            if manage.lower() in ['n', 'no']:
                print(f"[yellow]Skipping management of {obj_type}[/yellow]")
            else:
                managed_schema_objects.append(obj_type)
                print(f"[green]✔ Snow-Globe will manage {obj_type}[/green]")
        print("You can add or remove object_types anytime from config.yml")
    else:
        managed_schema_objects = schema_objects

    # environment configuration
    environments = []
    account = Prompt.ask(f"Enter Snowflake account identifier for your production environment: ")
    env_config = {
        "name": "prod",
        "account": account,
    }
    environments.append(env_config)
    manage_dev_environments = Prompt.ask(f"Do you want Snow-Globe to manage and/or deploy to development environments? [Y/n] ", default="Y")
    if manage_dev_environments.lower() in ['y', 'yes']:
        while True:
            try:
                env_counter = IntPrompt.ask("How many development environments will Snow-Globe be managing?")
                if env_counter < 1:
                    raise InvalidResponse("Please enter a number greater than 0.")
                if env_counter > 3:
                    print(
                        f"""
                        [yellow]
                        This wizard will setup the first 3 environments.
                        The additonal {env_counter-3} environments can be configured in the config.yml configration file.
                        [/yellow]
                        """
                    )
                    env_counter = 3
                break
            except (ValueError, InvalidResponse) as e:
                print(f"[red]Error:[/red] {e}")

    for env in range(env_counter):
        env_config = {}
        name = Prompt.ask(f"Enter name for environment {env+1} (e.g., dev, staging): ")
        env_config['name'] = name
        same_account = Prompt.ask(f"Does this environment use the same Production Snowflake account {account}? [Y/n] ")
        if same_account.lower() in ['n','no']:
            account = Prompt.ask(f"Enter Snowflake account identifier for {name} environment: ")
        env_config['account'] = account
        database_prefix_used = Prompt.ask(f"Does this environment use a database prefix? [Y/n] ", default="n")
        if database_prefix_used.lower() in ['y','yes']:
            database_prefix = Prompt.ask(f"Enter database prefix for {name} environment: ")
            env_config['database_prefix'] = database_prefix
        environments.append(env_config)
        print(f"[green]✔ Environment {name} configured![/green]")

    manage_all_databases = Prompt.ask(f"Do you want Snow-Globe to manage all databases? [Y/n] ", default="Y")
    if manage_all_databases.lower() in ['n','no']:
        database_schema = Prompt.ask(f"Enter database names (comma separated): ")
        managed_databases = [db.strip() for db in database_schema.split(",")]

    template = template_env.get_template('config.yml.j2')

    output = template.render(
        environments = environments,
        managed_databases = managed_databases,
        managed_schema_objects = managed_schema_objects
    )
    config_file_path = dest_dir / "config.yml"
    with config_file_path.open('w') as f:
        f.write(output)
