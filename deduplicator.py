"""ArchiveSweep: collision-safe file deduplication (bounded-window scheduler)."""

from __future__ import annotations

import hashlib
import os
import stat
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_CHUNK = 1024 * 1024
_CMP_CHUNK = 64 * 1024


@dataclass(frozen=True)
class DuplicateGroup:
    canonical: str
    duplicates: tuple[str, ...]
    size: int


@dataclass(frozen=True)
class FileIssue:
    path: str
    error: str


@dataclass(frozen=True)
class ScanReport:
    groups: tuple[DuplicateGroup, ...]
    issues: tuple[FileIssue, ...]
    files_considered: int
    bytes_hashed: int


def _fmt(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _read_up_to(f, n: int) -> bytes:
    parts, got = [], 0
    while got < n:
        b = f.read(n - got)
        if not b:  # EOF: stop immediately
            break
        parts.append(b)
        got += len(b)
    return b"".join(parts)


class _CompareError(Exception):
    def __init__(self, path: str, cause: BaseException):
        super().__init__(str(cause))
        self.path = path
        self.cause = cause


class FileDeduplicator:
    def __init__(self, max_workers: int = 4, sample_size: int = 4096):
        if max_workers < 1 or sample_size < 1:
            raise ValueError("limits must be positive")
        self.max_workers = max_workers
        self.sample_size = sample_size
        self.peak_in_flight = 0  # instrumentation: max futures alive at once

    # ---- scheduling window -------------------------------------------------
    def _window(self) -> int:
        """Max futures submitted-but-unfinished. Must be >= max_workers so
        max_workers blocked hooks can still run simultaneously."""
        return 2 * self.max_workers

    # ---- stage hooks -------------------------------------------------------
    def _full_digest(self, path: Path) -> tuple[str, int]:
        h = hashlib.sha256()
        total = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
                total += len(chunk)
        return h.hexdigest(), total

    def _sample_fingerprint(self, path: Path, size: int) -> tuple[str, int]:
        ss = self.sample_size
        h = hashlib.sha256()
        with open(path, "rb") as f:
            if size <= 2 * ss:
                data = _read_up_to(f, size)
                h.update(data)
                n = len(data)
            else:
                head = _read_up_to(f, ss)
                f.seek(size - ss)
                tail = _read_up_to(f, ss)
                h.update(head)
                h.update(tail)
                n = len(head) + len(tail)
        return h.hexdigest(), n

    # ---- discovery ---------------------------------------------------------
    def _discover(self, paths, issues: dict) -> dict:
        files: dict[str, int] = {}
        seen: set[str] = set()
        for raw in paths:
            stack = [str(Path(raw))]
            while stack:
                p = stack.pop()
                if p in seen:
                    continue
                seen.add(p)
                try:
                    st = os.lstat(p)
                except Exception as e:
                    issues[p] = _fmt(e)
                    continue
                mode = st.st_mode
                if stat.S_ISLNK(mode):
                    issues[p] = "symbolic link skipped"
                elif stat.S_ISREG(mode):
                    files[p] = st.st_size
                elif stat.S_ISDIR(mode):
                    try:
                        with os.scandir(p) as it:
                            names = [e.name for e in it]
                    except Exception as e:
                        issues[p] = _fmt(e)
                        continue
                    for name in names:
                        stack.append(str(Path(p) / name))
                else:
                    issues[p] = "special file skipped"
        return files

    # ---- exact verification ------------------------------------------------
    def _same_bytes(self, a: str, b: str) -> bool:
        try:
            fa = open(a, "rb")
        except Exception as e:
            raise _CompareError(a, e) from e
        with fa:
            try:
                fb = open(b, "rb")
            except Exception as e:
                raise _CompareError(b, e) from e
            with fb:
                while True:
                    try:
                        x = fa.read(_CMP_CHUNK)
                    except Exception as e:
                        raise _CompareError(a, e) from e
                    try:
                        y = fb.read(_CMP_CHUNK)
                    except Exception as e:
                        raise _CompareError(b, e) from e
                    if x != y:
                        return False
                    if not x:
                        return True

    def _partition(self, members: list, issues: dict) -> list:
        classes: list[list[str]] = []
        for p in sorted(members):
            placed = False
            ci = 0
            while ci < len(classes):
                cls = classes[ci]
                try:
                    equal = self._same_bytes(cls[0], p)
                except _CompareError as err:
                    issues[err.path] = _fmt(err.cause)
                    if err.path == p:
                        placed = True
                        break
                    cls.pop(0)
                    if not cls:
                        classes.pop(ci)
                    continue
                if equal:
                    cls.append(p)
                    placed = True
                    break
                ci += 1
            if not placed:
                classes.append([p])
        return [c for c in classes if len(c) > 1]

    # ---- scan --------------------------------------------------------------
    def scan(self, paths: Iterable[str | Path]) -> ScanReport:
        issues: dict[str, str] = {}
        files = self._discover(paths, issues)

        by_size: dict[int, list[str]] = defaultdict(list)
        for p, s in files.items():
            by_size[s].append(p)

        # Lazy producer: sample tasks are generated on demand, never
        # materialised as a list of futures.
        sample_iter = (
            (p, size)
            for size, group in by_size.items()
            if len(group) > 1  # unique sizes never reach hashing
            for p in group
        )
        full_queue: deque = deque()  # (path, size) waiting for a free slot

        sample_buckets: dict = defaultdict(list)
        full_buckets: dict = defaultdict(list)
        bytes_hashed = 0
        pending: dict = {}
        self.peak_in_flight = 0
        window = max(self._window(), self.max_workers)

        def fill(ex) -> None:
            # Full hashes first: finish work already invested in.
            while len(pending) < window:
                if full_queue:
                    q, size = full_queue.popleft()
                    fut = ex.submit(self._full_digest, Path(q))
                    pending[fut] = ("full", q, size)
                else:
                    nxt = next(sample_iter, None)
                    if nxt is None:
                        break
                    p, size = nxt
                    fut = ex.submit(self._sample_fingerprint, Path(p), size)
                    pending[fut] = ("sample", p, size)
            if len(pending) > self.peak_in_flight:
                self.peak_in_flight = len(pending)

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            fill(ex)
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    kind, p, size = pending.pop(fut)
                    try:
                        digest, n = fut.result()
                    except Exception as e:
                        issues[p] = _fmt(e)
                        continue
                    bytes_hashed += n
                    if kind == "sample":
                        bucket = sample_buckets[(size, digest)]
                        bucket.append(p)
                        if len(bucket) == 2:
                            full_queue.extend((q, size) for q in bucket)
                        elif len(bucket) > 2:
                            full_queue.append((p, size))
                    else:
                        full_buckets[(size, digest)].append(p)
                fill(ex)

        groups = []
        for (size, _), members in full_buckets.items():
            if len(members) < 2:
                continue
            for cls in self._partition(members, issues):
                cls.sort()
                groups.append(DuplicateGroup(cls[0], tuple(cls[1:]), size))
        groups.sort(key=lambda g: g.canonical)

        return ScanReport(
            groups=tuple(groups),
            issues=tuple(FileIssue(p, e) for p, e in sorted(issues.items())),
            files_considered=len(files),
            bytes_hashed=bytes_hashed,
        )
