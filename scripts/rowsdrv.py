"""Shared plumbing for the `scripts/perf_*.py` tools: build and run the `rows` driver."""

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CPU = 4
DEFAULT_FEATURES = "mimalloc"
HOT_SYMBOL = "consume::consume_blocks"  # matched as a suffix, see find_symbol

# Path prefixes that only add noise to source-line output.
_PREFIXES = re.compile(
    r"(?:/home/[^/]+/\.cargo/registry/src/[^/]+/"
    r"|/home/[^/]+/\.rustup/toolchains/[^/]+/lib/rustlib/src/rust/library/"
    r"|" + re.escape(str(ROOT)) + "/)"
)


def strip_paths(text: str) -> str:
    return _PREFIXES.sub("", text)


def add_common_args(parser) -> None:
    parser.add_argument("--bin", metavar="PATH", help="prebuilt driver; default builds the working tree")
    parser.add_argument("--iters", type=int, default=300, help="passes over the file per run (default 300)")
    parser.add_argument("--cpu", type=int, default=DEFAULT_CPU, help="core to pin to; negative disables pinning")
    parser.add_argument("--features", default=DEFAULT_FEATURES, help=f"cargo features (default {DEFAULT_FEATURES})")


def resolve_driver(args) -> Path:
    return Path(args.bin) if args.bin else build_driver(args.features)


def build_driver(features: str) -> Path:
    cmd = ["cargo", "bench", "--bench", "rows", "--no-run", "--message-format=json"]
    if features:
        cmd += ["--features", features]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(proc.stderr)
    for line in proc.stdout.splitlines():
        msg = json.loads(line)
        if msg.get("reason") == "compiler-artifact" and msg["target"]["name"] == "rows" and msg.get("executable"):
            return Path(msg["executable"])
    sys.exit("could not locate the built `rows` executable")


def driver_cmd(binary: Path, iters: int, cpu: int) -> list[str]:
    cmd = ["taskset", "-c", str(cpu)] if cpu >= 0 else []
    return cmd + [str(binary), str(iters)]


def total_rows(stdout: str) -> int:
    match = re.search(r"rows=(\d+) iters=(\d+)", stdout)
    if not match:
        sys.exit(f"driver did not report its row count:\n{stdout}")
    return int(match[1]) * int(match[2])


def perf_stat(binary: Path, iters: int, cpu: int, *stat_args: str) -> tuple[int, str]:
    """Run the driver under `perf stat -x,`; return (rows processed, perf CSV on stderr)."""
    cmd = ["perf", "stat", "-x,", *stat_args, "--", *driver_cmd(binary, iters, cpu)]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(proc.stderr)
    return total_rows(proc.stdout), proc.stderr


def parse_counters(csv: str) -> dict[str, float]:
    """`perf stat -x,` event rows -> {event: value}; unsupported events become NaN."""
    counters: dict[str, float] = {}
    for line in csv.splitlines():
        fields = line.split(",")
        if len(fields) < 3 or not fields[2]:
            continue
        value, event = fields[0], fields[2].removesuffix(":u")
        counters[event] = float("nan") if value.startswith("<") else float(value)
    return counters


def find_symbol(binary: Path, suffix: str) -> tuple[str, int, int]:
    """Locate the function whose demangled name ends with `suffix`; returns (name, start, size)."""
    nm = subprocess.run(["nm", "-C", "-S", str(binary)], capture_output=True, text=True, check=True).stdout
    hits = []
    for line in nm.splitlines():
        fields = line.split(maxsplit=3)
        if len(fields) == 4 and fields[2] in "tT" and (fields[3] == suffix or fields[3].endswith("::" + suffix)):
            hits.append((fields[3], int(fields[0], 16), int(fields[1], 16)))
    if len(hits) != 1:
        found = ", ".join(h[0] for h in hits) or "none"
        sys.exit(f"{binary}: expected one symbol matching {suffix!r}, found: {found}")
    return hits[0]
