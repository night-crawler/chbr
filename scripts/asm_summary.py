#!/usr/bin/env python3

import argparse
import collections
import difflib
import re
import subprocess
import sys
from pathlib import Path

import rowsdrv

STACK_STORE = re.compile(r"^mov\w*\s+%\w+,\s*(?:0x[0-9a-f]+)?\(%rsp\)")
STACK_LOAD = re.compile(r"^mov\w*\s+(?:0x[0-9a-f]+)?\(%rsp\),\s*%\w+")
NORMALISE = [
    (re.compile(r"^\s*[0-9a-f]+:\s*"), ""),
    (re.compile(r"<[^>]*>"), ""),
    (re.compile(r"#.*$"), ""),
    (re.compile(r"^(j\w+|call|jmp)\s+[0-9a-f]+.*"), r"\1 TARGET"),
    (re.compile(r"0x[0-9a-f]+"), "N"),
    (re.compile(r"%[re]?[abcd]x|%[re]?[sd]i|%[re]?[sb]p|%r\d+[dwb]?|%[abcd][lh]|%[sd]il|%xmm\d+"), "R"),
    (re.compile(r"^(nop\w*|xchg\s+%ax,%ax|data16).*"), "NOP"),
]


def disassemble(binary: Path, symbol: str) -> tuple[int, list[str]]:
    _, start, size = rowsdrv.find_symbol(binary, symbol)
    cmd = ["objdump", "-d", "--no-show-raw-insn", "-C", f"--start-address={start:#x}",
           f"--stop-address={start + size:#x}", str(binary)]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    insns = [line.strip() for line in out.splitlines() if re.match(r"\s*[0-9a-f]+:\s", line)]
    return size, insns


def normalise(insn: str) -> str:
    for pattern, repl in NORMALISE:
        insn = pattern.sub(repl, insn)
    return " ".join(insn.split())


def summarise(size: int, insns: list[str]) -> dict[str, int | str]:
    body = [re.sub(r"^\s*[0-9a-f]+:\s*", "", i) for i in insns]
    mnemonic = lambda i: i.split()[0] if i.split() else ""
    frame = next((m[1] for i in body if (m := re.match(r"sub\s+\$(0x[0-9a-f]+),%rsp", i))), "-")
    return {
        "bytes": size,
        "insns": len(body),
        "jmp*": sum(1 for i in body if re.match(r"jmp\s+\*", i)),
        "call": sum(1 for i in body if mnemonic(i) == "call"),
        "branches": sum(1 for i in body if mnemonic(i).startswith("j") and mnemonic(i) != "jmp"),
        "cmov": sum(1 for i in body if mnemonic(i).startswith("cmov")),
        "stack-st": sum(1 for i in body if STACK_STORE.match(i)),
        "stack-ld": sum(1 for i in body if STACK_LOAD.match(i)),
        "frame": frame,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bin", action="append", default=[], metavar="PATH",
                        help="binary to inspect (repeatable); the working-tree build is always included")
    parser.add_argument("--symbol", default=rowsdrv.HOT_SYMBOL,
                        help=f"function to inspect, matched as a `::`-suffix (default {rowsdrv.HOT_SYMBOL})")
    parser.add_argument("--features", default=rowsdrv.DEFAULT_FEATURES, help="cargo features for the build")
    parser.add_argument("--no-build", action="store_true", help="only inspect --bin binaries")
    parser.add_argument("--mnemonics", action="store_true", help="print the top mnemonics per binary")
    args = parser.parse_args()

    binaries: dict[str, Path] = {}
    if not args.no_build:
        binaries["current"] = rowsdrv.build_driver(args.features)
    for extra in args.bin:
        binaries[Path(extra).name] = Path(extra)
    if not binaries:
        sys.exit("nothing to inspect")

    listings = {name: disassemble(path, args.symbol) for name, path in binaries.items()}
    summaries = {name: summarise(*listing) for name, listing in listings.items()}

    name_width = max(len(name) for name in binaries)
    keys = list(next(iter(summaries.values())))
    print(f"{args.symbol}\n{'':{name_width}}  " + "  ".join(f"{k:>9}" for k in keys))
    for name, summary in summaries.items():
        print(f"{name:{name_width}}  " + "  ".join(f"{summary[k]!s:>9}" for k in keys))

    if args.mnemonics:
        for name, (_, insns) in listings.items():
            counts = collections.Counter(normalise(i).split()[0] for i in insns if normalise(i))
            print(f"\n{name}: " + "  ".join(f"{m} {n}" for m, n in counts.most_common(14)))

    if len(listings) == 2:
        (name_a, (_, a)), (name_b, (_, b)) = listings.items()
        norm_a = [normalise(i) for i in a if normalise(i) != "NOP"]
        norm_b = [normalise(i) for i in b if normalise(i) != "NOP"]
        removed, added = collections.Counter(), collections.Counter()
        for line in difflib.unified_diff(norm_a, norm_b, n=0, lineterm=""):
            if line.startswith("-") and not line.startswith("---"):
                removed[line[1:]] += 1
            elif line.startswith("+") and not line.startswith("+++"):
                added[line[1:]] += 1
        print(f"\ndiff {name_a} -> {name_b} (addresses/registers normalised, nops dropped): "
              f"-{sum(removed.values())} +{sum(added.values())} lines")
        for label, counter in (("removed", removed), ("added", added)):
            for insn, n in counter.most_common(10):
                print(f"  {label:7} {n:4}  {insn}")


if __name__ == "__main__":
    main()
