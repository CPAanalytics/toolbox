"""
txlookup  •  Search parquet GL files for a specific amount and return *all*
rows that share the same Tx-ID(s).

Example
-------
toolbox txlookup ./gl_export      \
        --amount-col Amount       \
        --tx-col TxID             \
        --lookup 1234.56          \
        --out matches.csv
"""

from pathlib import Path
from typing import Iterable, Set

import click
import pandas as pd


def _parquet_files(gl_path: Path) -> Iterable[Path]:
    """Yield every *.parquet file under *gl_path* (non-recursively)."""
    if gl_path.is_file() and gl_path.suffix == ".parquet":
        yield gl_path
    elif gl_path.is_dir():
        yield from sorted(gl_path.glob("*.parquet"))
    else:
        raise click.BadParameter(f"{gl_path} is neither a directory nor a parquet file.")


@click.command("txlookup")
@click.argument(
    "gl_path",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--amount-col",
    required=True,
    help="Column that holds the numeric amount.",
)
@click.option(
    "--tx-col",
    required=True,
    help="Column that holds the transaction identifier.",
)
@click.option(
    "--lookup",
    type=float,
    required=True,
    help="Exact amount to find (use negative value for debits).",
)
@click.option(
    "--out",
    "out_file",
    type=click.Path(writable=True, path_type=Path),
    help="Write matching rows to this file (CSV).  Defaults to STDOUT.",
)
def txlookup_cmd(
    gl_path: Path,
    amount_col: str,
    tx_col: str,
    lookup: float,
    out_file: Path | None,
):
    """
    Scan every parquet in *GL_PATH* and emit all rows whose *TX_COL* equals any
    Tx-ID where *AMOUNT_COL* == LOOKUP.
    """
    click.echo("🔍  Scanning GL files…", err=True)

    # Pass 1 – discover the set of Tx-IDs that contain the lookup amount
    target_txids: Set[str] = set()
    for f in _parquet_files(gl_path):
        df = pd.read_parquet(f, columns=[amount_col, tx_col])
        hits = df.loc[df[amount_col] == lookup, tx_col].dropna().unique()
        target_txids.update(map(str, hits))

    if not target_txids:
        click.echo("❌  No matching amount found in any file.", err=True)
        raise SystemExit(1)

    click.echo(f"✅  Found {len(target_txids)} Tx-ID(s) with amount {lookup}", err=True)

    # Pass 2 – pull every row whose Tx-ID is in the discovered set
    frames = []
    for f in _parquet_files(gl_path):
        df = pd.read_parquet(f)
        frames.append(df[df[tx_col].astype(str).isin(target_txids)])

    result = pd.concat(frames, ignore_index=True)

    # Output
    if out_file:
        result.to_csv(out_file, index=False)
        click.echo(f"📄  Wrote {len(result)} rows → {out_file}", err=True)
    else:
        result.to_csv(None, index=False)  # STDOUT


if __name__ == "__main__":
    txlookup_cmd()

