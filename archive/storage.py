"""Descriptor-anchored, no-follow bounded storage. No user filenames in destinations."""

import hashlib
import logging
import os
import stat
from pathlib import Path, PurePosixPath
from uuid import uuid4

FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW

log = logging.getLogger(__name__)


class StorageError(RuntimeError):
    pass


class CommitUnknown(RuntimeError):
    """The fence's COMMIT was sent but its outcome never arrived (connection lost)."""


def interrupted_commit(exc):
    """Mark an interruption (KeyboardInterrupt, SystemExit, CancelledError) raised
    while awaiting COMMIT: the outcome is unknown, so the linked file is kept. The
    exception itself is re-raised unchanged so shutdown still propagates.
    """
    exc.commit_unknown = True


def keep_linked(exc, reconcile):
    """Whether a linked file must survive the fence's failure.

    A confirmed rollback leaves nothing referencing the file: remove it. After an
    uncertain COMMIT only ``reconcile()`` (a fresh read of committed state) may say
    the file is unreferenced; if it cannot tell, the file is kept, because an orphan
    file is harmless and a committed reference to a deleted file is not.
    """
    if getattr(exc, "commit_unknown", False):
        # No reconciliation while shutting down: an orphan is acceptable.
        log.warning("publish_commit_interrupted file_kept")
        return True
    if not isinstance(exc, CommitUnknown):
        return False
    try:
        referenced = reconcile is None or reconcile()
    except Exception:
        log.warning("publish_reconcile_failed file_kept")
        return True
    log.info("publish_commit_unknown referenced=%s", bool(referenced))
    return bool(referenced)


def parts(path):
    values = PurePosixPath(path).parts
    if not values or ".." in values or "\x00" in str(path):
        raise StorageError("unsafe_path")
    return values


def directory(root, create=False):
    if not root.is_absolute():
        raise StorageError("absolute_root_required")
    fd = os.open("/", FLAGS)
    try:
        for part in parts(str(root))[1:]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            new = os.open(part, FLAGS, dir_fd=fd)
            os.close(fd)
            fd = new
        return fd
    except (OSError, ValueError):
        os.close(fd)
        raise StorageError("unsafe_directory") from None


def open_under(root: Path, absolute: str):
    try:
        raw = parts(absolute)
        base = parts(str(root))
        if raw[: len(base)] != base or len(raw) <= len(base):
            raise StorageError("outside_root")
        fd = directory(root)
        try:
            for part in raw[len(base) : -1]:
                new = os.open(part, FLAGS, dir_fd=fd)
                os.close(fd)
                fd = new
            file_fd = os.open(raw[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            if not stat.S_ISREG(os.fstat(file_fd).st_mode):
                os.close(file_fd)
                raise StorageError("not_regular")
            return os.fdopen(file_fd, "rb")
        finally:
            os.close(fd)
    except (OSError, ValueError):
        raise StorageError("unsafe_source") from None


def publish_stream(
    target_root, relative, chunks, maximum, check=lambda: None, fence=None, reconcile=None
):
    """Write chunks to a private temporary file, then publish without overwriting.

    The size bound is enforced while writing and the SHA-256 is computed over the
    exact stored bytes. ``fence(size, checksum, link)`` runs after size/checksum
    completion and owns the exclusive hardlink: it calls ``link()`` only while it
    holds whatever makes publication legitimate (lease, approval lock) and keeps
    holding it until the file is referenced. If ``fence`` raises after linking,
    the final file is removed again, unless it raises CommitUnknown and
    ``reconcile()`` does not confirm that no committed row references the file
    (see keep_linked). A stale lease owner never publishes.
    """
    dest = parts(relative)
    if dest[0] == "/":
        raise StorageError("relative_target_required")
    fd = directory(target_root, create=True)
    temp = ".partial-" + uuid4().hex
    linked = False

    def link():
        nonlocal linked
        # Hardlink is exclusive publication, unlike rename/replace. Never overwrite.
        os.link(temp, dest[-1], src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
        linked = True
        os.fsync(fd)

    try:
        for part in dest[:-1]:
            try:
                os.mkdir(part, 0o700, dir_fd=fd)
            except FileExistsError:
                pass
            new = os.open(part, FLAGS, dir_fd=fd)
            os.close(fd)
            fd = new
        out_fd = os.open(
            temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd
        )
        size = 0
        digest = hashlib.sha256()
        with os.fdopen(out_fd, "wb") as out:
            for chunk in chunks:
                check()
                size += len(chunk)
                if size > maximum:
                    raise StorageError("oversized")
                out.write(chunk)
                digest.update(chunk)
            out.flush()
            os.fsync(out.fileno())
        check()
        checksum = digest.hexdigest()
        if fence is None:
            link()
        else:
            fence(size, checksum, link)
        return size, checksum
    except BaseException as exc:
        if linked and not keep_linked(exc, reconcile):
            # Linked but never referenced (the fence's transaction failed): remove it.
            try:
                os.unlink(dest[-1], dir_fd=fd)
                os.fsync(fd)
            except OSError:
                pass
        if isinstance(exc, OSError):
            raise StorageError("file_io_error") from None
        raise
    finally:
        try:
            os.unlink(temp, dir_fd=fd)
        except FileNotFoundError:
            pass
        os.close(fd)
