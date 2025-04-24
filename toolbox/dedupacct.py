"""
dedupacct • Cancel duplicate Tx-IDs for one account over a date range.

The command:

1.  loads *.parquet* GL files (directory or single file);
2.  filters to the requested *account* & *date range*;
3.  **groups by a Tx-ID column**, summing the amount per Tx-ID;
4.  finds pairs where abs(sum) is equal and signs differ;
5.  drops *all* rows whose Tx-ID is in any cancelling pair.

Example
-------
toolbox dedupacct ./gl_parquet                    ^
        --acct-col "Account No"  --acct-num 4001  ^
        --date-col PostDate     --start 2024-01-01 --end 2024-03-31 ^
        --tx-col  TxID          --amount-col Amount                ^
        --out 4001_clean.csv
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple, Set

import click
import pandas as pd


# ───────────────────────── helper functions ──────────────────────────
def _parquet_files(path: Path) -> Iterable[Path]:
    """Yield each *.parquet file (non-recursive)."""
    if path.is_file() and path.suffix == ".parquet":
        yield path
    elif path.is_dir():
        yield from sorted(path.glob("*.parquet"))
    else:
        raise click.BadParameter(f"{path} is neither a directory nor a parquet file.")


def _find_pairs(df: pd.DataFrame, amt_col: str) -> List[Tuple[int, int]]:
    """Return list of (row_idx_pos, row_idx_neg) that cancel each other."""
    positives: dict[float, int] = {}
    negatives: dict[float, int] = {}
    pairs: list[Tuple[int, int]] = []

    for idx, amount in df[amt_col].items():
        key = abs(amount)
        if amount >= 0:
            neg_idx = negatives.pop(key, None)
            if neg_idx is not None:
                pairs.append((idx, neg_idx))
            else:
                positives[key] = idx
        else:
            pos_idx = positives.pop(key, None)
            if pos_idx is not None:
                pairs.append((pos_idx, idx))
            else:
                negatives[key] = idx
    return pairs


# ────────────────────────────  CLI  ──────────────────────────────────
@click.command("dedupacct")
@click.argument("gl_path", type=click.Path(exists=True, path_type=Path))
@click.option("--acct-col",  required=True, help="Column holding the account number.")
@click.option("--acct-num",  required=True, help="Account number to deduplicate.")
@click.option("--date-col",  required=True, help="Column with the posting date.")
@click.option("--start",     required=True, help="Inclusive start date  (YYYY-MM-DD).")
@click.option("--end",       required=True, help="Inclusive end   date  (YYYY-MM-DD).")
@click.option("--tx-col",    required=True, help="Column with the transaction identifier.")
@click.option("--amount-col", default="Amount", show_default=True,
              help="Numeric column with debits (-) and credits (+).")
@click.option("--out", "out_file",
              type=click.Path(writable=True, path_type=Path),
              help="Write cleaned data to this file.  Extension decides CSV/Parquet; "
                   "omit to stream CSV to STDOUT.")
def dedupacct_cmd(
    gl_path: Path,
    acct_col: str, acct_num: str,
    date_col: str, start: str, end: str,
    tx_col: str,
    amount_col: str,
    out_file: Path | None,
):
    """
    Remove Tx-IDs that cancel each other (equal magnitude, opposite sign)
    for *ACCT_NUM* between *START* and *END* inclusive.
    """
    click.echo("📂  Loading parquet files…", err=True)

    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)

    # 1⃣  Gather filtered rows (keep ALL original columns)
    frames = []
    for f in _parquet_files(gl_path):
        df = pd.read_parquet(f)
        mask = (
            (df[acct_col].astype(str) == str(acct_num)) &
            (pd.to_datetime(df[date_col]).between(start_dt, end_dt))
        )
        if mask.any():
            frames.append(df.loc[mask].copy())

    if not frames:
        click.echo("❌  No rows matched the account / date criteria.", err=True)
        raise SystemExit(1)

    df_acc = pd.concat(frames, ignore_index=True)

    # 2⃣  Build Tx-level summary
    df_acc[amount_col] = pd.to_numeric(df_acc[amount_col], errors="coerce")
    if df_acc[amount_col].isna().any():
        raise click.ClickException(f"Column '{amount_col}' contains non-numeric values.")

    summary = (
        df_acc
        .groupby(tx_col, dropna=False, sort=False)[amount_col]
        .sum()
        .reset_index()
        .rename(columns={amount_col: "_tx_sum_"})
    )

    # 3⃣  Find cancelling Tx-ID pairs
    pairs = _find_pairs(summary, "_tx_sum_")
    if not pairs:
        click.echo("ℹ️  No cancelling Tx-IDs found; file is unchanged.", err=True)
        cleaned = df_acc
    else:
        txids_to_drop: Set[str] = set(
            summary.loc[[i for pair in pairs for i in pair], tx_col].astype(str)
        )
        click.echo(f"🔍  Removing {len(txids_to_drop)} duplicate Tx-ID(s).", err=True)
        cleaned = df_acc[~df_acc[tx_col].astype(str).isin(txids_to_drop)]

    # 4⃣  Output
    if out_file:
        if out_file.suffix.lower() == ".parquet":
            cleaned.to_parquet(out_file, index=False)
        else:                              # default to CSV
            cleaned.to_csv(out_file, index=False)
        click.echo(f"✅  Wrote {len(cleaned)} rows → {out_file}", err=True)
    else:
        cleaned.to_csv(None, index=False)  # STDOUT

