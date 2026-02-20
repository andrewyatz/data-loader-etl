import argparse
import sys

DESCRIPTION = "Tool for generating AMR filter data"


def get_cli_args() -> argparse.ArgumentParser:
    """
    CLI arguments object
    """
    cli = argparse.ArgumentParser(prog=sys.argv[0], description=DESCRIPTION)

    cli.add_argument("-r", "--release", required=True, help="Release name")

    cli.add_argument(
        "-c",
        "--config",
        required=True,
        help="A JSON file that defines the filter categories and views",
    )

    cli.add_argument(
        "-d",
        "--data",
        required=True,
        help="A JSON file detailing data sources",
    )

    cli.add_argument(
        "--schema", default="schema", help="JSON Schema directory to validate input"
    )

    cli.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite any files or directories if already present",
    )

    cli.add_argument("-v", "--verbose", action="store_true", help="Be chatty")

    return cli
