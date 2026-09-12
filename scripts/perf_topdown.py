#!/usr/bin/env python3
"""Top-down pipeline breakdown of the row loop: where do the issue slots go?

Level 1 splits slots into retiring / frontend bound / backend bound / bad speculation;
`--l2` refines each (memory vs cpu backend stalls, mispredicts vs restarts, ...). Uses the
kernel's `perf stat -M PipelineL1|PipelineL2` metric groups, so the names are the CPU
vendor's. Level 2 multiplexes counters; run it a couple of times if a number looks odd.

    scripts/perf_topdown.py
    scripts/perf_topdown.py --l2 --bin /tmp/rows-before
"""

import argparse
import re

import rowsdrv

GROUPS = {"PipelineL1": "level 1", "PipelineL2": "level 2"}


def metrics(binary, iters: int, cpu: int, group: str) -> list[tuple[str, float]]:
    _, csv = rowsdrv.perf_stat(binary, iters, cpu, "-M", group)
    found = []
    for line in csv.splitlines():
        fields = line.split(",")
        # metric rows end with `<value>,%  <metric name>`; the unit and name share a field
        if len(fields) >= 7 and fields[5] and fields[6].startswith("%"):
            found.append((fields[6].removeprefix("%").strip(), float(fields[5])))
    if not found:
        raise SystemExit(f"perf produced no {group} metrics; `perf list metricgroup` shows what this CPU offers")
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    rowsdrv.add_common_args(parser)
    parser.set_defaults(iters=100)
    parser.add_argument("--l2", action="store_true", help="also run the level-2 breakdown")
    args = parser.parse_args()

    binary = rowsdrv.resolve_driver(args)
    groups = ["PipelineL1"] + (["PipelineL2"] if args.l2 else [])
    for group in groups:
        print(GROUPS[group])
        for name, pct in sorted(metrics(binary, args.iters, args.cpu, group), key=lambda m: -m[1]):
            bar = "#" * round(pct / 2)
            print(f"  {name:36} {pct:5.1f}%  {bar}")


if __name__ == "__main__":
    main()
