# ArchiveSweep: bounded-window scheduler

Collision-safe file deduplication (size filter, non-overlapping sample, streaming SHA-256,
byte-for-byte verification) using one `ThreadPoolExecutor` and a **bounded submission window**.

## What changed

The first version submitted one future per candidate file up front, so scheduler memory grew
with the file count. Now `scan` keeps at most `window = 2 * max_workers` futures alive:

- Sample tasks come from a lazy generator, not a list of futures.
- Files whose sample digests match go into a small `deque`; full-hash tasks are submitted from it first.
- Whenever futures finish, `fill()` tops the window back up.
- The window is never below `max_workers`, so `max_workers` blocked hooks can still run together.

## Results (real output of `demo_bounded_window.py`)

# Bounded-window results

`max_workers=4`, window = `2 * max_workers` = 8

| files | strategy | peak futures in flight | peak traced MB | seconds | groups | issues |
|---:|---|---:|---:|---:|---:|---:|
| 2000 | upfront | 2000 | 4.4 | 0.27 | 1 | 0 |
| 2000 | bounded | 8 | 1.6 | 0.28 | 1 | 0 |
| 10000 | upfront | 10000 | 21.1 | 1.42 | 1 | 0 |
| 10000 | bounded | 8 | 3.8 | 1.39 | 1 | 0 |
| 30000 | upfront | 30000 | 65.9 | 4.40 | 1 | 0 |
| 30000 | bounded | 8 | 12.0 | 3.98 | 1 | 0 |

- barrier of 1 blocked sample hooks releases: **True**
- barrier of 2 blocked sample hooks releases: **True**
- barrier of 4 blocked sample hooks releases: **True**
- barrier of 8 blocked sample hooks releases: **True**

## Caveats

- In-flight futures are O(`max_workers`). Discovery still stores one path and size per file, and
  sample buckets store paths, so total memory is still O(files) in path strings, just far smaller
  than one future per file. A truly O(1) walk would stream discovery lazily too.
- Hard links are reported as duplicates (path identity, not inode identity).

## Run

    python3 demo_bounded_window.py
    python3 -m pytest -q test_bounded.py
