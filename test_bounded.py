from deduplicator import FileDeduplicator


def test_groups_and_missing(tmp_path):
    (tmp_path / "a.txt").write_bytes(b"same")
    (tmp_path / "c.txt").write_bytes(b"same")
    (tmp_path / "b.txt").write_bytes(b"othr")
    (tmp_path / "d.txt").write_bytes(b"othr")
    r = FileDeduplicator().scan([tmp_path, tmp_path / "missing.txt"])
    assert [g.canonical for g in r.groups] == [str(tmp_path / "a.txt"), str(tmp_path / "b.txt")]
    assert len(r.issues) == 1 and r.files_considered == 4


def test_in_flight_is_bounded(tmp_path):
    for i in range(500):
        (tmp_path / f"f{i}").write_bytes(f"{i:0>32}".encode())
    dd = FileDeduplicator(max_workers=3)
    dd.scan([tmp_path])
    assert 3 <= dd.peak_in_flight <= 6


def test_window_never_below_workers(tmp_path):
    class Tiny(FileDeduplicator):
        def _window(self):
            return 1
    for i in range(20):
        (tmp_path / f"f{i}").write_bytes(b"x" * 10)
    dd = Tiny(max_workers=4)
    dd.scan([tmp_path])
    assert dd.peak_in_flight == 4
