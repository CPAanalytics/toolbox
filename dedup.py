from pathlib import Path
from typing import List, Tuple

import click
import pandas as pd


def _find_pairs(df: pd.DataFrame, amt_col: str, show_progress: bool = False) -> List[Tuple[int, int]]:
    """
    Return list of (row_index_pos, row_index_neg) that cancel each other.
    Matching key is just abs(amount).
    """
    positives: dict[float, int] = {}
    negatives: dict[float, int] = {}
    pairs: list[Tuple[int, int]] = []

    # Prepare iterable, optionally wrapped in a progress bar
    iterable = df[amt_col].items()
    if show_progress:
        iterable = click.progressbar(iterable, length=len(df), label="Finding cancelling pairs")

    for idx, amount in iterable:
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


@click.command("dedup")
@click.argument(
    "csv_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--amount",
    "amt_col",
    default="Amount",
    show_default=True,
    help="Numeric column with debits (-) and credits (+).",
)
@click.option(
    "--out",
    "out_file",
    type=click.Path(writable=True, path_type=Path),
    help="Write cleaned CSV here (prints to STDOUT if omitted).",
)
def dedup_cmd(csv_file: Path, amt_col: str, out_file: Path | None):
    """Remove cancelling in/out pairs from *CSV_FILE*."""
    df = pd.read_csv(csv_file)

    if amt_col not in df.columns:
        raise click.BadParameter(f"'{amt_col}' not in columns: {', '.join(df.columns)}")

    df[amt_col] = pd.to_numeric(df[amt_col], errors="coerce")
    if df[amt_col].isna().any():
        raise click.ClickException(f"Column '{amt_col}' contains non-numeric values.")

    # Find cancelling pairs with a progress bar
    pairs = _find_pairs(df, amt_col, show_progress=True)

    # Drop matched rows
    cleaned = df.drop(index=[i for p in pairs for i in p]) if pairs else df

    if out_file:
        cleaned.to_csv(out_file, index=False)
        click.echo(f"✅  Wrote cleaned file to {out_file}")
    else:
        cleaned.to_csv(None, index=False)


if __name__ == "__main__":
    dedup_cmd()

