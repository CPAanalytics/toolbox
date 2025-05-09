import click
from .dedupcsv import dedup_cmd
from .dedupacct import dedupacct_cmd
from .txlookup import txlookup_cmd

@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
def cli():
    """Collection of small CLI helpers for the accounting team."""
    pass

cli.add_command(dedup_cmd)
cli.add_command(txlookup_cmd)
cli.add_command(dedupacct_cmd)
