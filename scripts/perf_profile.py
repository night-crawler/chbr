#!/usr/bin/env python3
"""Where the cycles go: `perf record` the rows driver, attribute samples to source lines.

    scripts/perf_profile.py                  # top source lines
    scripts/perf_profile.py --symbols        # also per-function breakdown
    scripts/perf_profile.py --annotate       # also hottest instructions in the row loop
    scripts/perf_profile.py --bin /tmp/rows-before --top 30

Percentages are shares of all samples in the run. Lines from `hint.rs` are the
`black_box` sinks in the consumer, not library cost.
"""

import argparse
import re
import subprocess
import tempfile
from pathlib import Path

import rowsdrv


def perf_report(data: Path, sort: str, limit: float) -> list[tuple[float, str]]:
    cmd = ["perf", "report", "-i", str(data), "--no-children", "--stdio", "-g", "none",
           "--sort", sort, "--percent-limit", str(limit), "--full-source-path"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    rows = []
    for line in out.splitlines():
        match = re.match(r"\s*([0-9.]+)%\s+(?:\[\.\]\s+)?(.*)", line)
        if match:
            where = rowsdrv.strip_paths(match[2].strip())
            # `file:0` is code with no line info (compiler-generated glue, drop, moves)
            rows.append((float(match[1]), where.replace(":0", "  (no line info)") if where.endswith(":0") else where))
    return rows


def perf_annotate(data: Path, symbol: str, top: int) -> list[str]:
    cmd = ["perf", "annotate", "-i", str(data), "--stdio", "-l", "--full-paths", "-s", symbol]
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    hot = []
    for line in out.splitlines():
        match = re.match(r"\s*([0-9.]+)\s*:\s+([0-9a-f]+):\s+(.*)", line)
        if match and float(match[1]) > 0:
            hot.append((float(match[1]), match[2], rowsdrv.strip_paths(match[3])))
    hot.sort(reverse=True)
    return [f"{pct:6.2f}%  {addr}: {re.sub(r' <[^>]*>', '', insn)}" for pct, addr, insn in hot[:top]]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    rowsdrv.add_common_args(parser)
    parser.add_argument("--top", type=int, default=20, help="rows to print per section (default 20)")
    parser.add_argument("--freq", type=int, default=4999, help="sampling frequency in Hz (default 4999)")
    parser.add_argument("--symbols", action="store_true", help="also print the per-function breakdown")
    parser.add_argument("--annotate", metavar="SYMBOL", nargs="?", const=rowsdrv.HOT_SYMBOL,
                        help=f"also print the hottest instructions of SYMBOL (default {rowsdrv.HOT_SYMBOL})")
    args = parser.parse_args()

    binary = rowsdrv.resolve_driver(args)
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "perf.data"
        cmd = ["perf", "record", "-q", "-o", str(data), "-F", str(args.freq), "--",
               *rowsdrv.driver_cmd(binary, args.iters, args.cpu)]
        subprocess.run(cmd, cwd=rowsdrv.ROOT, check=True, capture_output=True)

        print("source lines")
        for pct, line in perf_report(data, "srcline", 0.3)[: args.top]:
            print(f"{pct:6.2f}%  {line}")

        if args.symbols:
            print("\nfunctions")
            for pct, sym in perf_report(data, "sym", 0.3)[: args.top]:
                print(f"{pct:6.2f}%  {sym}")

        if args.annotate:
            symbol, _, _ = rowsdrv.find_symbol(binary, args.annotate)
            print(f"\nhottest instructions in {symbol}")
            for line in perf_annotate(data, symbol, args.top):
                print(line)


if __name__ == "__main__":
    main()
