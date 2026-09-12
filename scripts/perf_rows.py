#!/usr/bin/env python3
"""Per-row hardware counters for the derive read path.

Runs the fixed-work driver (`benches/rows.rs`) under `perf stat` pinned to one core and
prints cycles, instructions, IPC, branches and mispredicts per row. Extra binaries (e.g. a
copy of an earlier build) are interleaved with the current one for A/B runs:

    scripts/perf_rows.py
    scripts/perf_rows.py --runs 5 --bin /tmp/rows-before
    scripts/perf_rows.py --event ls_any_fills_from_sys.all   # AMD: L1 fills from anywhere
"""

import argparse
import statistics
from pathlib import Path

import rowsdrv

BASE_EVENTS = ["cycles", "instructions", "branches", "branch-misses"]


def run_once(binary: Path, iters: int, cpu: int, events: list[str]) -> dict[str, float]:
    rows, csv = rowsdrv.perf_stat(binary, iters, cpu, "-e", ",".join(f"{e}:u" for e in events))
    per_row = {event: value / rows for event, value in rowsdrv.parse_counters(csv).items()}
    per_row["ipc"] = per_row["instructions"] / per_row["cycles"]
    return per_row


def columns(events: list[str]) -> list[tuple[str, str, str]]:
    cols = [
        ("cycles", "cycles/row", "{:10.1f}"),
        ("instructions", "instr/row", "{:9.0f}"),
        ("ipc", "IPC", "{:5.2f}"),
        ("branches", "branches/row", "{:12.1f}"),
        ("branch-misses", "misp/row", "{:8.2f}"),
    ]
    for event in events:
        if event not in BASE_EVENTS:
            title = f"{event}/row"
            cols.append((event, title, "{:" + str(len(title)) + ".2f}"))
    return cols


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=3, help="rounds per binary, interleaved (default 3)")
    parser.add_argument("--iters", type=int, default=300, help="passes over the file per run (default 300)")
    parser.add_argument("--cpu", type=int, default=rowsdrv.DEFAULT_CPU, help="core to pin to; negative disables pinning")
    parser.add_argument("--features", default=rowsdrv.DEFAULT_FEATURES, help="cargo features for the build")
    parser.add_argument("--bin", action="append", default=[], metavar="PATH", help="extra prebuilt driver to compare against")
    parser.add_argument("--event", action="append", default=[], help="extra perf event, normalised per row")
    args = parser.parse_args()

    events = BASE_EVENTS + args.event
    binaries = {"current": rowsdrv.build_driver(args.features)}
    for extra in args.bin:
        binaries[Path(extra).name] = Path(extra)

    cols = columns(events)
    name_width = max(len(name) for name in binaries)
    print(f"{'':{name_width}}  " + "  ".join(f"{title:>{len(fmt.format(0))}}" for _, title, fmt in cols))

    results: dict[str, list[dict[str, float]]] = {name: [] for name in binaries}
    for _ in range(args.runs):
        for name, binary in binaries.items():
            counters = run_once(binary, args.iters, args.cpu, events)
            results[name].append(counters)
            print(f"{name:{name_width}}  " + "  ".join(fmt.format(counters[key]) for key, _, fmt in cols))

    if args.runs > 1:
        print(f"\n{'median':{name_width}}")
        for name, runs in results.items():
            med = {key: statistics.median(run[key] for run in runs) for key, _, _ in cols}
            print(f"{name:{name_width}}  " + "  ".join(fmt.format(med[key]) for key, _, fmt in cols))


if __name__ == "__main__":
    main()
