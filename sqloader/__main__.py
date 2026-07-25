import argparse
import os
import sys

from .sqloader import SQLoader
from .dialect import DialectConverter


def cmd_convert(args):
    conv = DialectConverter(args.from_db, args.to)

    def _convert_text(text):
        out = conv.convert(text, placeholders=args.placeholders)
        for w in conv.warnings:
            print(f"  WARN: {w}", file=sys.stderr)
        return out

    if os.path.isdir(args.path):
        out_dir = args.out or args.path
        converted = 0
        for root, _, files in os.walk(args.path):
            for filename in files:
                if not filename.endswith(".sql"):
                    continue
                src_path = os.path.join(root, filename)
                rel_path = os.path.relpath(src_path, args.path)
                dst_path = os.path.join(out_dir, rel_path)
                with open(src_path, "r", encoding="utf-8") as f:
                    result = _convert_text(f.read())
                os.makedirs(os.path.dirname(dst_path) or ".", exist_ok=True)
                with open(dst_path, "w", encoding="utf-8") as f:
                    f.write(result)
                converted += 1
                print(f"Converted {rel_path}")
        print(f"Converted {converted} file(s): {args.from_db} -> {args.to}")
        return

    if os.path.isfile(args.path):
        with open(args.path, "r", encoding="utf-8") as f:
            result = _convert_text(f.read())
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(result)
            print(f"Converted {args.path} -> {args.out} ({args.from_db} -> {args.to})")
        else:
            sys.stdout.write(result)
            if not result.endswith("\n"):
                sys.stdout.write("\n")
        return

    print(f"Error: path not found: {args.path}", file=sys.stderr)
    sys.exit(1)


def cmd_sync(args):
    path = args.path or os.getcwd()
    sq = SQLoader(path)

    try:
        result = sq.sync(args.from_db, args.to, overwrite=args.overwrite,
                         convert=args.convert)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    copied = result["copied"]
    skipped = result["skipped"]
    warnings = result.get("warnings", [])

    mode = "converted" if args.convert else "copied"
    print(f"Synced {args.from_db} -> {args.to}" + (" (with dialect conversion)" if args.convert else ""))
    print(f"{mode.capitalize()}: {len(copied)} files")
    for f in copied:
        print(f"  - {f}")
    print(f"Skipped: {len(skipped)} files")
    for f in skipped:
        print(f"  - {f}")
    for w in warnings:
        print(f"  WARN: {w}", file=sys.stderr)


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
    sync_parser.add_argument("--convert", action="store_true",
                             help="Convert .sql file contents between the --from and "
                                  "--to dialects while syncing (.json copied verbatim)")
    sync_parser.set_defaults(func=cmd_sync)

    convert_parser = subparsers.add_parser(
        "convert", help="Convert .sql files between SQL dialects")
    convert_parser.add_argument("--from", dest="from_db", required=True, metavar="DIALECT",
                                help="Source dialect (sqlite, mysql/mariadb, postgresql)")
    convert_parser.add_argument("--to", required=True, metavar="DIALECT",
                                help="Target dialect (sqlite, mysql/mariadb, postgresql)")
    convert_parser.add_argument("--path", required=True, metavar="PATH",
                                help="Source .sql file or directory")
    convert_parser.add_argument("--out", default=None, metavar="PATH",
                                help="Output file/dir (default: stdout for a file, in-place for a dir)")
    convert_parser.add_argument("--placeholders", action="store_true",
                                help="Also translate parameter placeholders (? <-> %%s)")
    convert_parser.set_defaults(func=cmd_convert)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
