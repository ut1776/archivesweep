"""Compare upfront submission vs. bounded window; prints a markdown report."""

import os
import tempfile
import threading
import time
import tracemalloc
from pathlib import Path

from deduplicator import FileDeduplicator


class UpfrontDeduplicator(FileDeduplicator):
    """Old behaviour: window so large every task is submitted immediately."""

    def _window(self) -> int:
        return 10**9


def build_tree(root: Path, n: int) -> int:
    """n files, all 64 bytes, distinct content except every 100th is a copy."""
    dups = 0
    for i in range(n):
        d = root / f"d{i % 50}"
        d.mkdir(exist_ok=True)
        body = f"{i:0>64}".encode()
        if i % 100 == 0 and i:
            body = f"{0:0>64}".encode()
            dups += 1
        (d / f"f{i}.bin").write_bytes(body)
    return dups


def run(cls, root, workers):
    dd = cls(max_workers=workers)
    tracemalloc.start()
    t0 = time.perf_counter()
    rep = dd.scan([root])
    dt = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return dd.peak_in_flight, peak / 1e6, dt, rep


def concurrency_check(root, workers):
    """Block sample hooks on a barrier sized to max_workers."""
    barrier = threading.Barrier(workers, timeout=5)

    class Blocked(FileDeduplicator):
        def _sample_fingerprint(self, path, size):
            barrier.wait()
            return super()._sample_fingerprint(path, size)

    rep = Blocked(max_workers=workers).scan([root])
    return len(rep.issues) == 0 and len(rep.groups) > 0


if __name__ == "__main__":
    workers = 4
    print("# Bounded-window results\n")
    print(f"`max_workers={workers}`, window = `2 * max_workers` = {2 * workers}\n")
    print("| files | strategy | peak futures in flight | peak traced MB | seconds | groups | issues |")
    print("|---:|---|---:|---:|---:|---:|---:|")
    for n in (2000, 10000, 30000):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = build_tree(root, n)
            for label, cls in (("upfront", UpfrontDeduplicator),
                               ("bounded", FileDeduplicator)):
                inflight, mb, dt, rep = run(cls, root, workers)
                print(f"| {n} | {label} | {inflight} | {mb:.1f} | {dt:.2f} | "
                      f"{len(rep.groups)} | {len(rep.issues)} |")
    print()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i in range(8):
            (root / f"x{i}").write_bytes(b"same-content")
        for w in (1, 2, 4, 8):
            ok = concurrency_check(root, w)
            print(f"- barrier of {w} blocked sample hooks releases: **{ok}**")
