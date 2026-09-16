"""``edgefaultlab`` command line interface.

    edgefaultlab validate scenario.json
    edgefaultlab run scenario.json
    edgefaultlab run scenario.json --output runs/test-001 --seed 123

``validate`` exists because a scenario is a program, and a program that is only
checked by running the whole system is a slow way to find a typo in a field
name.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from . import __version__
from .report import CONSOLE_EVENTS, format_event, render_header
from .runner import DEFAULT_RUNS_DIR, ScenarioRunner
from .scenario import Scenario, ScenarioError, load_scenario

__all__ = ["main"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edgefaultlab",
        description=(
            "Deterministic fault injection and resilience testing for "
            "distributed Edge / IoT systems."
        ),
    )
    parser.add_argument("--version", action="version", version=f"edgefaultlab {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="check a scenario file without running it")
    validate.add_argument("scenario", help="path to a scenario JSON file")
    validate.set_defaults(func=cmd_validate)

    run = subparsers.add_parser("run", help="run a scenario")
    run.add_argument("scenario", help="path to a scenario JSON file")
    run.add_argument(
        "-o",
        "--output",
        help=f"run directory (default: {DEFAULT_RUNS_DIR}/<run id>)",
    )
    run.add_argument("--seed", type=int, help="override the seed of the scenario")
    run.add_argument(
        "-q", "--quiet", action="store_true", help="do not echo events while running"
    )
    run.set_defaults(func=cmd_run)
    return parser


def cmd_validate(args: argparse.Namespace) -> int:
    try:
        scenario = load_scenario(args.scenario)
    except ScenarioError as exc:
        print(exc, file=sys.stderr)
        return 1
    print(f"OK  {scenario.name}")
    print(f"    duration   : {scenario.duration:g}s")
    print(f"    seed       : {scenario.seed}")
    print(f"    links      : {', '.join(scenario.link_names)}")
    if scenario.processes:
        print(f"    processes  : {', '.join(proc.name for proc in scenario.processes)}")
    print(f"    faults     : {len(scenario.faults)}")
    print(f"    assertions : {len(scenario.assertions)}")
    return 0


def _echo(record: dict) -> None:
    if record["event"] in CONSOLE_EVENTS:
        print(format_event(record), flush=True)


def cmd_run(args: argparse.Namespace) -> int:
    try:
        scenario: Scenario = load_scenario(args.scenario)
    except ScenarioError as exc:
        print(exc, file=sys.stderr)
        return 2

    seed = scenario.seed if args.seed is None else args.seed
    print(render_header(scenario, seed, __version__), flush=True)
    runner = ScenarioRunner(
        scenario,
        output=args.output,
        seed=args.seed,
        echo=None if args.quiet else _echo,
        log=lambda message: print(message, file=sys.stderr, flush=True),
    )
    exit_code = asyncio.run(runner.run())
    print(runner.console)
    print(f"\nRun directory: {runner.run_dir}")
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
