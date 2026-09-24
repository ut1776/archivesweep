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

## Design reflection

### Files that change during a scan

The scanner records a file's size once, at discovery, and reopens the file separately for the sample, the full hash, and the byte comparison. A file that changes in between can therefore produce a stale size, a digest that no longer matches the size, or an I/O error. Errors such as a deleted file or a truncated read are caught per file and reported as a `FileIssue`, and the rest of the scan continues.

Silent changes are harder. The byte-for-byte comparison is the final safeguard, because it compares the contents as they are at that moment, so a hash computed on old contents cannot cause a false duplicate. It still cannot promise that the file stays unchanged after the comparison ends.

To detect changes more reliably, I would record `st_size` and `st_mtime_ns` (and inode, where available) at discovery and re-check them right before and after each stage. If they differ, the file is dropped from grouping and reported as an issue such as "modified during scan". Files being actively written should be excluded or rescanned. For a strict guarantee, I would scan a snapshot (LVM, ZFS, or a filesystem snapshot) instead of the live tree.

### Hard links, inode identity, and repeated paths

Repeated paths and hard links are different problems. A repeated path is the same file listed twice, for example through overlapping roots. The scanner uses `str(Path(path))` as identity and processes each identity once, so the same path never appears as its own duplicate and is not hashed twice.

Hard links are different paths that point to the same inode and the same data. This implementation treats them as separate paths with equal contents, so they show up as duplicates. That is accurate about content, but it overstates storage savings: deleting one hard link frees no space, because the data blocks are shared.

A more accurate design would identify files by `(st_dev, st_ino)`. Hard links to one inode would be collapsed to a single representative, so the data is hashed and compared only once. The extra link paths would then be reported separately as "already the same storage", not counted as reclaimable duplicates. The choice depends on what the report is for: if the user wants a list of equal-content paths, keeping hard links is fine, and if the goal is space savings, they must be excluded from the savings figure. Inode identity is only meaningful within one device, and some filesystems (network shares, FAT) do not provide stable inode numbers, so the fallback there is content comparison.

### Queue, open-file, and worker bounds by storage type

Each bound protects a different resource: worker count limits concurrent I/O, the open-file limit protects file descriptors, and the queue bound limits memory used by pending work. I would set them based on the storage type.

- **Local SSD/NVMe:** These handle parallel reads well, so a worker count near the CPU core count (SHA-256 becomes CPU-bound) is reasonable. Larger chunks, such as 1 MiB, cut syscall overhead. Open files stay comfortably below `ulimit -n`.
- **Local spinning disks:** Parallel reads cause seek thrashing, so I would use one or two workers and sort work by path or inode to keep reads mostly sequential.
- **Network filesystems (NFS/SMB):** Latency per open and per read dominates, so more workers can hide the latency, but too many overload the server or hit its limits. I would use a moderate worker count, larger reads, and timeouts, and treat transient errors as retryable issues.
- **Object storage (S3-style):** Each read is an HTTP request with per-request latency and cost. Sampling and size filtering matter even more here, because they avoid full downloads. I would use high concurrency, since requests are independent, but with rate limiting. Ranged reads for the head and tail samples avoid fetching whole objects, and metadata (size, ETag) can prefilter candidates.

In all cases the work queue should be bounded, not one future per file, so that a scan of millions of files cannot exhaust memory. This repo does that: `scan` keeps at most `2 * max_workers` futures in flight, so scheduler memory no longer grows with the file count. The per-file path and size bookkeeping is still held in memory, so total memory is still O(files), just much smaller than before.
