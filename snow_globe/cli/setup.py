# cli/init.py
import shutil
import typer
from pathlib import Path
from rich import print
from importlib.resources import files

setup_cmd = typer.Typer()

@setup_cmd.command("init")
def init(
    target_dir: Path = typer.Option(
        Path.cwd(), "--path", "-p", help="Directory to initialize the project"
    )
):
    """
    Initialize Snow-Globe in a directory by creating profiles.yml and scaffold folders.
    """
    # Files to copy from package to target
    files_to_copy = ["profiles.yml", "config.yml"]

    for filename in files_to_copy:
        dest_file = target_dir / filename
        if dest_file.exists():
            print(f"[yellow]{filename} already exists in {target_dir}. Skipping.[/yellow]")
            continue

        try:
            template = files("snow_globe").joinpath(filename)
        except Exception as e:
            print(f"[red]Error finding bundled {filename}: {e}[/red]")
            raise typer.Exit(code=1)

        shutil.copy(template, dest_file)
        print(f"[green]✔ Created {filename} at {dest_file}[/green]")

    # Directories to create
    dirs_to_create = ["ddl_management", "logs", "data"]

    for folder in dirs_to_create:
        folder_path = target_dir / folder
        folder_path.mkdir(exist_ok=True)
        print(f"[green]✔ Created {folder}/ folder[/green]")
