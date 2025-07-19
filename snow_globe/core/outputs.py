# core/outputs.py
def print_table(df, title=None, no_wrap=True):

    import pandas as pd
    from rich.table import Table
    from rich.console import Console

    console = Console()
    table = Table(title=title or "", header_style="bold cyan", row_styles=["none", "dim"])

    for col in df.columns:
        table.add_column(str(col), no_wrap=no_wrap, max_width=None if not no_wrap else 80)

    for row in df.itertuples(index=False):
        formatted_row = [
            str(cell).replace("\n", " ").replace("\r", " ") if no_wrap else str(cell)
            for cell in row
        ]
        table.add_row(*formatted_row)

    console.print(table)


def print_plan(plan: dict):

    from typer import style, echo
    from typer.colors import GREEN, RED, YELLOW

    echo(style(f"{'-'*22} PLAN {'-'*22}", bold=True))
    if not plan['new_objects'] and not plan['modified_objects'] and not plan['deleted_objects']:
        echo(style("• No changes to state!",fg=GREEN))
    if plan['new_objects']:
        echo(style(f"Add:",fg=GREEN))
        for obj in plan['new_objects']:
            file = obj['fqn']
            val = obj.get('validation')
            msg = obj.get('message')
            echo(
                f"\t• {file}"
                + style(f" [{val}]", fg=RED if val == "ERROR" else GREEN)
            )
            if msg:
                indented_msg = "\n".join(f"\t\t {line}" for line in msg.splitlines())
                echo(indented_msg)
    if plan['modified_objects']:
        echo(style("Modify:",fg=YELLOW))
        for obj in plan['modified_objects']:
            file = obj['fqn']
            val = obj.get('validation')
            if obj.get('alter_reason'):
                echo(f"Refresh required: {obj['reason']}")
            msg = obj.get('message')
            echo(
                f"\t• {file}"
                + style(f" [{val}]", fg=RED if val == "ERROR" else GREEN)
            )
            if msg:
                indented_msg = "\n".join(f"\t\t {line}" for line in msg.splitlines())
                echo(indented_msg)
    if plan['deleted_objects']:
        echo(style("Drop:",fg=RED))
        for obj in plan['deleted_objects']:
            file = obj['fqn']
            val = obj.get('validation')
            msg = obj.get('message')
            echo(
                f"\t• {file}"
                + style(f" [{val}]", fg=RED if val == "WARNING object reference found:" else GREEN)
            )
            if msg:
                indented_msg = "\n".join(f"\t\t {line}" for line in msg)
                echo(indented_msg)

    echo(style(f"{'-'*50}\n", bold=True))
    echo(
        f"Plan:"
        + style(f" {len(plan['new_objects'])} to add,",fg=GREEN)
        + style(f" {len(plan['modified_objects'])} to modify",fg=YELLOW)
        + style(f" {len(plan['deleted_objects'])} to drop",fg=RED)
    )

