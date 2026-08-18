#!/usr/bin/env python3
"""Fail CI on any govulncheck finding that is not in the reviewed allowlist.

govulncheck has no ignore mechanism, and several findings SAGE inherits have no
upstream fix at all (see .github/vuln-allowlist.txt). Running it bare would mean
a permanently red job, which is the same as no job. This filters to findings
that are actually *called* from SAGE's own code, subtracts the reviewed set, and
fails on the remainder.

Reads govulncheck -format json on stdin.
"""

import json
import pathlib
import sys

ALLOWLIST = pathlib.Path(__file__).resolve().parent.parent / "vuln-allowlist.txt"


def allowed() -> dict[str, str]:
    entries = {}
    for line in ALLOWLIST.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        osv, _, reason = line.partition("#")
        entries[osv.strip()] = reason.strip()
    return entries


def called(stream: str) -> set[str]:
    """OSV IDs whose trace reaches a named function — i.e. code we call."""
    decoder = json.JSONDecoder()
    found, i = set(), 0
    while i < len(stream):
        while i < len(stream) and stream[i].isspace():
            i += 1
        if i >= len(stream):
            break
        obj, i = decoder.raw_decode(stream, i)
        finding = obj.get("finding")
        if finding and finding.get("trace") and finding["trace"][0].get("function"):
            found.add(finding["osv"])
    return found


def main() -> int:
    accepted = allowed()
    hits = called(sys.stdin.read())
    new = sorted(hits - accepted.keys())
    stale = sorted(accepted.keys() - hits)

    for osv in sorted(hits & accepted.keys()):
        print(f"accepted  {osv}  {accepted[osv]}")
    for osv in stale:
        print(f"stale     {osv}  no longer reported — drop it from the allowlist")

    if new:
        print()
        for osv in new:
            print(f"NEW       {osv}  https://pkg.go.dev/vuln/{osv}")
        print(
            f"\n{len(new)} unreviewed vulnerability finding(s) reachable from SAGE code.\n"
            "Fix them, or add each to .github/vuln-allowlist.txt with the reason it is acceptable."
        )
        return 1

    print(f"\nno unreviewed findings ({len(hits)} accepted)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
