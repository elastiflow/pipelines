#!/usr/bin/env python3
import sys
import shlex
import subprocess
import argparse
from dataclasses import dataclass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--makefile", help="Makefile to run", required=True)
    parser.add_argument("--threshold", help="Test coverage threshold", required=True, type=float)
    parser.add_argument(
        "--go-mods", dest="go_mods", nargs="+", help="Go packages to check test threshold against", required=True
    )
    return parser.parse_args()


@dataclass
class PkgCoverage:
    """Class for keeping track of a package test coverage."""

    pkg: str
    coverage: float
    failed: bool  # True if coverage is less than threshold


def run_cmd(cmd: str):
    c = shlex.split(cmd)
    res = subprocess.run(c, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Error running command: {cmd}")
        print(f"stdout: {res.stdout}")
        print(f"stderr: {res.stderr}")
        sys.exit(1)
    return res


def coverage_threshold(pkg: str):
    """
    Get the total coverage for a given package and generate HTML report
    """
    res = run_cmd(f"make -f {args.makefile} test-unit-report-coverage GO_TEST_COVERAGE_MOD={pkg}")
    # Total coverage contains "total  "
    total_coverage = "0%"
    for i in res.stdout.strip().split("\n"):
        if "total  " in i.replace("\t", " "):
            total_coverage = i
            break

    coverage = float(total_coverage.split()[-1].strip("%"))
    run_cmd(f"make -f {args.makefile} test-unit-report-coverage-html GO_TEST_COVERAGE_MOD={pkg}")
    return coverage


args = parse_args()
pkgs = [PkgCoverage(pkg, (cov := coverage_threshold(pkg)), cov < args.threshold) for pkg in args.go_mods]
for p in pkgs:
    print(
        f'Test coverage for module "{p.pkg}" {"failed" if p.failed else "succeeded"} - '
        + f"expected > {args.threshold}% / actual = {p.coverage}%"
    )

failed_pkgs = [p for p in pkgs if p.failed]
if failed_pkgs:
    print("\nTests coverage threshold failed for modules:")
    print("\n".join(f"  {p.pkg}: {p.coverage}%" for p in failed_pkgs))
    sys.exit(1)
