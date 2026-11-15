"""Source manifest and secret/content gate: only known files, no token-shaped strings."""

import re
import subprocess
from pathlib import Path

allowed = {
    ".gitignore",
    ".githooks/pre-push",
    ".env.example",
    "settings.example.toml",
    "LICENSE",
    "README.md",
    "AGENTS.md",
    "docs/architecture.md",
    "docs/database.md",
    "docs/operations.md",
    "pyproject.toml",
    "uv.lock",
    "Dockerfile",
    "compose.yaml",
    ".dockerignore",
}
prefixes = ("archive/", "tests/", "migrations/", "scripts/")
paths = (
    subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    .decode()
    .split("\0")
)
for name in set(paths) - {""}:
    if name not in allowed and not name.startswith(prefixes):
        raise SystemExit("Source manifest rejects path: " + name)
    if name not in allowed and Path(name).suffix not in {".py", ".sh", ".sql"}:
        raise SystemExit("Source manifest rejects non-source file: " + name)
    path = Path(name)
    if not path.is_file() or path.is_symlink():
        raise SystemExit("Source path missing or not regular: " + name)
    data = path.read_bytes()
    if re.search(rb"[0-9]{8,10}:[A-Za-z0-9_-]{35}", data):
        raise SystemExit("Bot-token-shaped content rejected: " + name)
    if re.search(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----", data):
        raise SystemExit("Private key rejected: " + name)
print("PASS: source manifest and source-secret gate")
