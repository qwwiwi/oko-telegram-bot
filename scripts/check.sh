#!/bin/sh
# Never connects to a supplied DSN or an existing database service.
set -eu
cd "$(dirname "$0")/.."
umask 077
export LC_ALL=C
export LANG=C
git diff --check
git diff --cached --check
if git rev-parse --verify origin/main >/dev/null 2>&1; then git diff --check origin/main...HEAD; fi
UV=${UV:-$(command -v uv || true)}
test -n "$UV" || { printf '%s\n' 'uv required' >&2; exit 1; }
if test -z "${POSTGRES_BIN:-}"; then
    for candidate in /opt/homebrew/opt/postgresql@16/bin /usr/local/opt/postgresql@16/bin /usr/lib/postgresql/16/bin; do
        if test -x "$candidate/initdb"; then POSTGRES_BIN=$candidate; break; fi
    done
fi
if test -z "${POSTGRES_BIN:-}"; then
    POSTGRES_BIN=$(dirname "$(command -v initdb || printf /missing/initdb)")
fi
for tool in initdb pg_ctl postgres createdb; do
    test -x "$POSTGRES_BIN/$tool" || { printf 'PostgreSQL prerequisite missing: %s\n' "$tool" >&2; exit 1; }
done
case "$("$POSTGRES_BIN/postgres" --version)" in *' 16.'*) ;; *) printf '%s\n' 'PostgreSQL 16 required' >&2; exit 1;; esac
: "${TMPDIR:?TMPDIR must point to a private scratch directory}"
root=$(mktemp -d "${TMPDIR%/}/archive-test.XXXXXX")
cleanup() {
    "$POSTGRES_BIN/pg_ctl" -D "$root/db" -m immediate -w stop >/dev/null 2>&1 || true
    rm -rf "$root"
}
trap cleanup EXIT HUP INT TERM
"$POSTGRES_BIN/initdb" -D "$root/db" -A trust --no-locale -E UTF8 >"$root/initdb.log"
if ! "$POSTGRES_BIN/pg_ctl" -D "$root/db" -l "$root/postgres.log" -o "-F -c listen_addresses='' -c unix_socket_directories='$root' -c unix_socket_permissions=0700" -w start; then
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "$root/postgres.log"
    exit 1
fi
"$POSTGRES_BIN/createdb" -h "$root" archive_synthetic_test
export ARCHIVE_DISPOSABLE_TEST_ROOT="$root"
export ARCHIVE_DISPOSABLE_TEST_DSN="host=$root dbname=archive_synthetic_test"
"$UV" sync --locked --python 3.12
.venv/bin/python scripts/design_check.py
.venv/bin/python scripts/compose_check.py
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/pytest
.venv/bin/python scripts/api_smoke.py
printf '%s\n' 'PASS: lint + real disposable PostgreSQL integration + localhost API/file smoke'
