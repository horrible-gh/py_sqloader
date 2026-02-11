import argparse
import os
import sys

from .sqloader import SQLoader


def cmd_sync(args):
    path = args.path or os.getcwd()
    sq = SQLoader(path)

    try:
        result = sq.sync(args.from_db, args.to, overwrite=args.overwrite)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    copied = result["copied"]
    skipped = result["skipped"]

    print(f"Synced {args.from_db} -> {args.to}")
    print(f"Copied: {len(copied)} files")
    for f in copied:
        print(f"  - {f}")
    print(f"Skipped: {len(skipped)} files")
    for f in skipped:
        print(f"  - {f}")


def main():
    parser = argparse.ArgumentParser(
        prog="python -m sqloader",
        description="SQLoader CLI",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    sync_parser = subparsers.add_parser("sync", help="Sync query files between DB directories")
    sync_parser.add_argument("--from", dest="from_db", required=True, metavar="DB",
                             help="Source DB type (e.g. sqlite3, mysql, postgresql)")
    sync_parser.add_argument("--to", required=True, metavar="DB",
                             help="Target DB type")
    sync_parser.add_argument("--path", default=None, metavar="PATH",
                             help="SQL directory path (default: current directory)")
    sync_parser.add_argument("--overwrite", action="store_true",
                             help="Overwrite existing files")
    sync_parser.set_defaults(func=cmd_sync)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
