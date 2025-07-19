# snow_globe.py
import typer
from snow_globe.cli.setup import setup_cmd
from snow_globe.cli.state import state_cmd
from snow_globe.cli.trace import trace_cmd

app = typer.Typer()
app.add_typer(setup_cmd, name="setup")
app.add_typer(state_cmd, name="state")
app.add_typer(trace_cmd, name="trace")

if __name__ == "__main__":
    app()
