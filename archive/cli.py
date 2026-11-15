"""Operator CLI. No command implicitly enrolls, logs out, publishes or deploys."""

import argparse
import json
import sys
from pathlib import Path

import uvicorn

from archive.config import load
from archive.db import connect, migrate


def main():
    parser = argparse.ArgumentParser(prog="oko")
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("migrate", "poller", "worker", "status"):
        commands.add_parser(name)
    api = commands.add_parser("api")
    api.add_argument("--host", default="127.0.0.1", choices=("127.0.0.1", "0.0.0.0"))
    api.add_argument("--port", type=int, default=8610)
    api.add_argument("--fd", type=int)
    export = commands.add_parser("export")
    export.add_argument("--chat", type=int, required=True)
    export.add_argument("--date", required=True)
    transcripts = commands.add_parser("transcripts")
    actions = transcripts.add_subparsers(dest="transcripts_command", required=True)
    requeue = actions.add_parser("requeue")
    requeue.add_argument("--chat", type=int)
    requeue.add_argument("--dry-run", action="store_true")
    audit = commands.add_parser("audit")
    audit_actions = audit.add_subparsers(dest="audit_command", required=True)
    prune = audit_actions.add_parser("prune")
    prune.add_argument("--days", type=int, default=180, choices=range(1, 3651), metavar="DAYS")
    args = parser.parse_args()
    try:
        cfg = load(args.config)
        if args.command == "api":
            from archive.api import create_app

            uvicorn.run(
                create_app(cfg),
                host=args.host,
                port=args.port,
                fd=args.fd,
                access_log=False,
                log_level="warning",
                log_config=None,
            )
        elif args.command == "poller":
            from archive.poller import run

            run(cfg)
        elif args.command == "worker":
            from archive.worker import run

            run(cfg)
        else:
            with connect(cfg.dsn) as conn:
                if args.command == "migrate":
                    migrate(conn)
                    print("migration_ok")
                elif args.command == "status":
                    from archive.status import status

                    print(json.dumps(status(conn), default=str, sort_keys=True))
                elif args.command == "transcripts":
                    from archive.transcripts import requeue

                    counts = requeue(conn, cfg, args.chat, args.dry_run)
                    print(
                        f"requeued {counts.requeued}, "
                        f"skipped_submitted {counts.skipped_submitted}, "
                        f"skipped_mismatch {counts.skipped_mismatch}"
                    )
                elif args.command == "audit":
                    from archive.audit import prune

                    print(f"pruned {prune(conn, args.days)}")
                else:
                    from archive.export import export_day

                    export_day(conn, cfg, args.chat, args.date)
                    print("export_ok")
    except KeyboardInterrupt:
        return
    except Exception:
        print("archive_command_failed", file=sys.stderr)
        raise SystemExit(1) from None
