"""Incremental-maintenance and schema-migration tests.

These pin the core requirement of the redesign: when one tree's coring
changes, maintaining that year's distribution must scan and rewrite a number
of records that does **not** grow with the year's sample-tree count --
including when a median value appears for the first time or disappears
completely. They also cover aggregate-only migration of legacy data volumes.
"""

from __future__ import annotations

import random
import sqlite3
import statistics

import pytest

from app.chronology import (
    BITS,
    BIT_DOMAIN,
    WriteMetrics,
    apply_report,
    query_reference,
)
from app.db import init_db
from app.models import ReportRequest

LEGACY_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
INSERT INTO meta(key, value) VALUES ('data_version', '7');
CREATE TABLE core (
    tree_no INTEGER NOT NULL, core_no INTEGER NOT NULL, revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL, data_version INTEGER NOT NULL,
    PRIMARY KEY (tree_no, core_no)) WITHOUT ROWID;
CREATE TABLE measurement (
    tree_no INTEGER NOT NULL, core_no INTEGER NOT NULL, year INTEGER NOT NULL,
    index_value INTEGER NOT NULL, PRIMARY KEY (tree_no, core_no, year),
    FOREIGN KEY (tree_no, core_no) REFERENCES core(tree_no, core_no) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX measurement_tree_year ON measurement(tree_no, year);
CREATE TABLE tree_year (
    year INTEGER NOT NULL, tree_no INTEGER NOT NULL, median_x2 INTEGER NOT NULL,
    core_count INTEGER NOT NULL, PRIMARY KEY (year, tree_no)) WITHOUT ROWID;
CREATE INDEX tree_year_by_tree ON tree_year(tree_no, year);
CREATE INDEX tree_year_value ON tree_year(year, median_x2);
CREATE TABLE year_stat (
    year INTEGER PRIMARY KEY, tree_count INTEGER NOT NULL,
    slot_count INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE year_value_cum (
    year INTEGER NOT NULL, slot INTEGER NOT NULL, value_x2 INTEGER NOT NULL,
    cnt INTEGER NOT NULL, cum INTEGER NOT NULL, PRIMARY KEY (year, slot),
    UNIQUE (year, value_x2)) WITHOUT ROWID;
"""


def _one_core_report(tree_no, years):
    return ReportRequest(
        cores=[{"tree_no": tree_no, "core_no": 1, "years": {str(y): v for y, v in years.items()}}]
    )


def _build_stand(path, n_trees, year=2020):
    """n_trees with distinct per-tree medians in one year (value = tree_no)."""
    for start in range(1, n_trees + 1, 2000):
        apply_report(
            path,
            ReportRequest(
                cores=[
                    {"tree_no": t, "core_no": 1, "years": {str(year): t}}
                    for t in range(start, min(start + 2000, n_trees + 1))
                ]
            ),
        )


def test_year_maintenance_cost_is_constant_in_sample_tree_count(mods):
    path = mods["path"]
    costs = []
    for n in (100, 1000, 5000):
        _build_stand(path, n)
        # Tree 1's median moves from 1 to 99999: one -1 update and one +1.
        metrics = WriteMetrics()
        apply_report(path, _one_core_report(1, {2020: 99999}), metrics)
        costs.append((n, metrics.bit_node_touches, metrics.bit_rows_written))
        # The full year (all n tree medians) must never be read back.
        assert metrics.tree_year_rows_scanned == 1
        assert metrics.measurement_rows_scanned == 1

    touches = {t for _, t, _ in costs}
    written = {w for _, _, w in costs}
    # Two Fenwick updates (remove old median, insert new one), each at most
    # BITS nodes: the *same* bound at every stand size, i.e. independent of the
    # year's sample-tree count.
    assert len(touches) == 1
    assert max(touches) <= 2 * BITS
    assert len(written) == 1
    assert max(written) <= 2 * BITS

    # And the answers stay exact while the stand grows 50x.
    body = query_reference(path, 2020, None)
    assert body["sample_trees"] == 5000
    assert body["value"] == statistics.median(
        [99999] + list(range(2, 5001))
    )


def test_first_appearance_and_disappearance_are_also_constant(mods):
    path = mods["path"]
    n = 3000
    _build_stand(path, n)
    # Every existing tree uses values 1..n. Pick a value nobody holds: it
    # appears for the first time when tree 1 reports it.
    novel = n + 500

    appear = WriteMetrics()
    apply_report(path, _one_core_report(1, {2020: novel}), appear)
    assert appear.bit_median_changes == 1
    assert appear.bit_node_touches <= 2 * BITS  # remove old + insert novel
    assert query_reference(path, 2020, None)["trees_in_year"] == n

    # Tree 1 drops the year entirely: its unique value disappears completely.
    disappear = WriteMetrics()
    apply_report(path, _one_core_report(1, {}), disappear)
    assert disappear.bit_median_changes == 1
    assert disappear.bit_node_touches <= BITS  # one negative update only

    body = query_reference(path, 2020, None)
    assert body["trees_in_year"] == n - 1
    assert body["sample_trees"] == n - 1
    assert body["value"] == statistics.median(list(range(2, n + 1)))


def test_write_plan_uses_only_point_lookups(mods):
    path = mods["path"]
    _build_stand(path, 500)
    conn = mods["db"].connect_read(path, restricted=False)
    try:
        for sql, params in [
            (
                "SELECT cnt FROM year_value_bit WHERE year=? AND node=?",
                (2020, 1),
            ),
            (
                "SELECT median_x2 FROM tree_year WHERE year=? AND tree_no=?",
                (2020, 1),
            ),
            (
                "SELECT tree_count FROM year_stat WHERE year=?",
                (2020,),
            ),
        ]:
            detail = " ".join(
                r["detail"]
                for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
            )
            assert "SCAN" not in detail, detail
            assert "SEARCH" in detail, detail
    finally:
        conn.close()


def test_year_emptied_by_rereport_leaves_no_aggregate_behind(mods):
    path = mods["path"]
    # One tree with two cores in 2020; then re-report both without 2020.
    apply_report(
        path,
        ReportRequest(
            cores=[
                {"tree_no": 1, "core_no": 1, "years": {"2020": 10, "2021": 5}},
                {"tree_no": 1, "core_no": 2, "years": {"2020": 20}},
            ]
        ),
    )
    apply_report(
        path,
        ReportRequest(
            cores=[
                {"tree_no": 1, "core_no": 1, "years": {"2021": 6}},
                {"tree_no": 1, "core_no": 2, "years": {}},
            ]
        ),
    )
    conn = mods["db"].connect_read(path, restricted=False)
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM year_stat WHERE year=2020"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM year_value_bit WHERE year=2020"
        ).fetchone()["n"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM tree_year WHERE year=2020"
        ).fetchone()["n"] == 0
    finally:
        conn.close()

    body = query_reference(path, 2020, None)
    assert body["sample_trees"] == 0
    assert body["status"] == "insufficient_samples"
    assert body["value"] is None


def _seed_legacy_db(path):
    """Create a volume in the pre-redesign cumulative-histogram schema."""
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(LEGACY_SCHEMA)
        # Five trees, one core each, two years. year 2000: values 10..50;
        # year 2001: values 1..5.
        rows = []
        for t in range(1, 6):
            conn.execute(
                "INSERT INTO core VALUES (?,?,?,?,?)",
                (t, 1, 1, "2020-01-01T00:00:00+00:00", 7),
            )
            conn.execute("INSERT INTO measurement VALUES (?,?,?,?)", (t, 1, 2000, 10 * t))
            conn.execute("INSERT INTO measurement VALUES (?,?,?,?)", (t, 1, 2001, t))
            conn.execute("INSERT INTO tree_year VALUES (?,?,?,?)", (2000, t, 20 * t, 1))
            conn.execute("INSERT INTO tree_year VALUES (?,?,?,?)", (2001, t, 2 * t, 1))
            rows.append(t)
        for year, multiplier in ((2000, 20), (2001, 2)):
            cum = 0
            for slot, t in enumerate(rows, start=1):
                cum += 1
                conn.execute(
                    "INSERT INTO year_value_cum VALUES (?,?,?,?,?)",
                    (year, slot, multiplier * t, 1, cum),
                )
            conn.execute("INSERT INTO year_stat VALUES (?,?,?)", (year, 5, 5))
        conn.commit()
    finally:
        conn.close()


def test_legacy_volume_migrates_from_aggregates_only(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    _seed_legacy_db(path)
    result = apply_report(
        path,
        ReportRequest(cores=[{"tree_no": 6, "core_no": 1, "years": {"2000": 30}}]),
    )
    # Migration + one report, one transaction; version bumps once, to 8.
    assert result["data_version"] == 8

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        tables = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert "year_value_cum" not in tables
        assert "year_value_bit" in tables
        assert conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()[0] == "2"
        # Fenwick root counts all sample trees per year.
        root_2000 = conn.execute(
            "SELECT cnt FROM year_value_bit WHERE year=2000 AND node=?",
            (BIT_DOMAIN,),
        ).fetchone()["cnt"]
        assert root_2000 == 6
        root_2001 = conn.execute(
            "SELECT cnt FROM year_value_bit WHERE year=2001 AND node=?",
            (BIT_DOMAIN,),
        ).fetchone()["cnt"]
        assert root_2001 == 5
        # Raw measurements were preserved untouched.
        assert conn.execute("SELECT COUNT(*) AS n FROM measurement").fetchone()["n"] == 11
        assert conn.execute("SELECT COUNT(*) AS n FROM core").fetchone()["n"] == 6
    finally:
        conn.close()

    # Exact answers after migration + incremental report. Year 2000 actual
    # values (one core per tree, so medians equal the values):
    # 10,20,30,40,50 plus tree 6's 30 -> sorted 10,20,30,30,40,50;
    # even median = (30+30)/2 = 30.0.
    body = query_reference(path, 2000, None)
    assert body["sample_trees"] == 6
    assert body["value"] == 30.0
    body = query_reference(path, 2000, 6)
    assert body["sample_trees"] == 5
    assert body["value"] == 30.0
    body = query_reference(path, 2001, 2)
    assert body["sample_trees"] == 4
    # values 1,2,3,4,5 minus tree 2 -> 1,3,4,5 -> median (3+4)/2 = 3.5
    assert body["value"] == pytest.approx(3.5)


def test_failed_migration_preserves_committed_data_and_version(tmp_path, monkeypatch):
    path = tmp_path / "legacy-fail.sqlite3"
    _seed_legacy_db(path)

    import app.chronology as chronology

    real_bit_add = chronology._bit_add

    def boom(conn, year, value_x2, delta, metrics):
        raise sqlite3.OperationalError("simulated migration failure")

    monkeypatch.setattr(chronology, "_bit_add", boom)
    with pytest.raises(sqlite3.OperationalError):
        apply_report(path, ReportRequest(cores=[{"tree_no": 9, "core_no": 1, "years": {"2000": 1}}]))
    monkeypatch.setattr(chronology, "_bit_add", real_bit_add)

    conn = sqlite3.connect(str(path))
    try:
        # Legacy schema and the committed version are fully preserved.
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "year_value_cum" in names
        assert "year_value_bit" not in names
        assert conn.execute(
            "SELECT value FROM meta WHERE key='data_version'"
        ).fetchone()[0] == "7"
        assert conn.execute("SELECT COUNT(*) FROM measurement").fetchone()[0] == 10
        assert conn.execute("SELECT COUNT(*) FROM year_value_cum").fetchone()[0] == 10
        assert conn.execute("SELECT tree_count FROM year_stat WHERE year=2000").fetchone()[0] == 5
    finally:
        conn.close()

    # A later, healthy report migrates and succeeds.
    result = apply_report(
        path, ReportRequest(cores=[{"tree_no": 7, "core_no": 1, "years": {"2000": 35}}])
    )
    assert result["data_version"] == 8
    body = query_reference(path, 2000, None)
    assert body["sample_trees"] == 6


def test_large_randomized_medians_and_exclusions_against_bruteforce(tmp_path):
    """2k trees, 1-3 cores each, random values; exclusions and single-tree
    re-reports must stay exact while Fenwick write cost stays bounded."""
    path = tmp_path / "big.sqlite3"
    init_db(path)
    rnd = random.Random(42)
    series: dict[int, list[int]] = {}
    for tree in range(1, 2001):
        cores = []
        vals = []
        for cno in range(1, rnd.randint(1, 3) + 1):
            v = rnd.randrange(0, 2000)
            vals.append(v)
            cores.append({"tree_no": tree, "core_no": cno, "years": {"2000": v}})
        series[tree] = vals
        apply_report(path, ReportRequest(cores=cores))

    def brute(exclude):
        meds = [statistics.median(v) for t, v in series.items() if t != exclude]
        n = len(meds)
        return n, (statistics.median(meds) if n >= 3 else None)

    for exclude in [None, *[rnd.randrange(1, 2001) for _ in range(20)], 9999]:
        body = query_reference(path, 2000, exclude)
        n, val = brute(exclude)
        assert body["sample_trees"] == n
        if val is not None:
            assert body["value"] == pytest.approx(val)

    max_touches = 0
    for _ in range(20):
        tree = rnd.randrange(1, 2001)
        new_value = rnd.randrange(0, 2000)
        metrics = WriteMetrics()
        apply_report(
            path,
            ReportRequest(cores=[{"tree_no": tree, "core_no": 1, "years": {"2000": new_value}}]),
            metrics,
        )
        max_touches = max(max_touches, metrics.bit_node_touches)
        series[tree][0] = new_value
        body = query_reference(path, 2000, None)
        _, val = brute(None)
        assert body["value"] == pytest.approx(val)

    # Two median changes (remove old, insert new), each <= BITS nodes.
    assert max_touches <= 2 * BITS


def test_startup_migrates_legacy_volume_and_health_reports_ok(tmp_path):
    """The lifespan startup hook upgrades a legacy volume before serving."""
    from fastapi.testclient import TestClient

    path = tmp_path / "startup-legacy.sqlite3"
    _seed_legacy_db(path)

    import app.settings as settings_mod
    import app.main as main_mod

    settings_mod.settings = settings_mod.Settings(db_path=path)
    main_mod.settings = settings_mod.settings
    try:
        with TestClient(main_mod.app) as client:
            health = client.get("/health").json()
            assert health["schema_ok"] is True
            # Legacy histogram gone, Fenwick table live, answers exact.
            body = client.get("/chronology/2000").json()
            assert body["sample_trees"] == 5
            assert body["value"] == 30.0
            assert "year_value_cum" not in body
    finally:
        from app.settings import get_settings

        settings_mod.settings = get_settings()
        main_mod.settings = settings_mod.settings


def test_query_refuses_unmigrated_legacy_volume(tmp_path):
    """Without migration, a query must error rather than return garbage."""
    path = tmp_path / "unmigrated.sqlite3"
    _seed_legacy_db(path)
    import sqlite3 as _sqlite3

    with pytest.raises(_sqlite3.DatabaseError):
        query_reference(str(path), 2000, None)
