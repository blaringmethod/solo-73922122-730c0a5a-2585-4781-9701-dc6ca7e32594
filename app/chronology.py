"""Chronology ingest and reference-value queries.

Aggregate layout
----------------
``tree_year``     one row per (year, tree): median of that tree's core indices,
                  stored doubled so an even number of cores stays exact.
``year_stat``     one row per year that still has sample trees: tree count.
``year_value_bit``
                  sparse Fenwick (binary indexed) tree over a year's doubled
                  tree medians. Only non-zero nodes are stored.

Incremental maintenance
-----------------------
A report only changes the within-tree medians of the (tree, year) pairs its
cores touch. For every such pair we recompute *that one tree's* median from
its own measurements (a point lookup, independent of how many trees the year
has) and then apply the old -> new median difference to the year's Fenwick
tree:

* a median value appearing for the first time / disappearing completely is a
  single +1 / -1 update;
* a value changing is one -1 followed by one +1.

Each Fenwick update walks at most ``BITS`` (32) parent nodes, so the number of
aggregate rows scanned and rewritten while maintaining one year is O(BITS) per
changed tree median -- it does not grow with the year's sample-tree count. The
order-statistic query likewise walks at most ``BITS`` nodes per selection and
never touches ``measurement``/``core``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from .db import (
    BIT_TABLE,
    LEGACY_HISTOGRAM_TABLE,
    SCHEMA_VERSION,
    ReadSession,
    busy_retry,
    connect_write,
    read_snapshot,
    table_exists,
)
from .models import MAX_INDEX_VALUE, ReportRequest

MIN_TREES = 3

# The Fenwick tree spans BIT indices 1..BIT_DOMAIN where a doubled median v is
# stored at index v + 1. Doubled medians are at most 2 * MAX_INDEX_VALUE, so a
# 32-bit domain with signed-64-safe node ids is sufficient.
BITS = 32
BIT_DOMAIN = 1 << BITS
MAX_MEDIAN_X2 = BIT_DOMAIN - 1


# ---------------------------------------------------------------------------
# Write-side accounting
# ---------------------------------------------------------------------------


@dataclass
class WriteMetrics:
    """Rows the write path actually scanned/rewrote.

    Every counter is independent of the year's sample-tree count: only the
    touched tree's own measurements and O(BITS) Fenwick nodes per changed
    median are ever read or written.
    """

    measurement_rows_scanned: int = 0
    tree_year_rows_scanned: int = 0
    tree_year_pairs_recomputed: int = 0
    bit_median_changes: int = 0
    bit_node_touches: int = 0
    bit_rows_written: int = 0
    year_stat_rows_written: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "measurement_rows_scanned": self.measurement_rows_scanned,
            "tree_year_rows_scanned": self.tree_year_rows_scanned,
            "tree_year_pairs_recomputed": self.tree_year_pairs_recomputed,
            "bit_median_changes": self.bit_median_changes,
            "bit_node_touches": self.bit_node_touches,
            "bit_rows_written": self.bit_rows_written,
            "year_stat_rows_written": self.year_stat_rows_written,
        }


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _meta_int(conn: sqlite3.Connection, key: str) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return int(row["value"]) if row is not None else 0


def _set_meta_int(conn: sqlite3.Connection, key: str, value: int) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def _median_x2(values: list[int]) -> int:
    """Median of integers in doubled units (exact for even counts too)."""
    n = len(values)
    if n == 0:
        raise ValueError("median of empty sequence")
    if n % 2:
        return 2 * values[n // 2]
    return values[n // 2 - 1] + values[n // 2]


# ---------------------------------------------------------------------------
# Fenwick (binary indexed) tree maintenance
# ---------------------------------------------------------------------------


def _bit_add(
    conn: sqlite3.Connection,
    year: int,
    value_x2: int,
    delta: int,
    metrics: WriteMetrics | None,
) -> None:
    """Add ``delta`` (>=1 or <=-1) for one doubled median value in a year."""
    if not (0 <= value_x2 <= MAX_MEDIAN_X2):
        raise ValueError(f"median_x2 {value_x2} outside Fenwick domain")
    node = value_x2 + 1  # BIT indices are 1-based; value 0 sits at index 1
    while node <= BIT_DOMAIN:
        if metrics is not None:
            metrics.bit_node_touches += 1
        row = conn.execute(
            "SELECT cnt FROM year_value_bit WHERE year=? AND node=?",
            (year, node),
        ).fetchone()
        if delta > 0:
            if row is None:
                conn.execute(
                    "INSERT INTO year_value_bit(year, node, cnt) VALUES(?,?,?)",
                    (year, node, delta),
                )
            else:
                conn.execute(
                    "UPDATE year_value_bit SET cnt=cnt+? WHERE year=? AND node=?",
                    (delta, year, node),
                )
        else:
            if row is None:  # pragma: no cover - defensive invariant
                raise sqlite3.DatabaseError(
                    f"Fenwick node missing for negative delta: year={year} node={node}"
                )
            new_cnt = int(row["cnt"]) + delta
            if new_cnt == 0:
                # Sparse storage: nodes that no longer contribute disappear.
                conn.execute(
                    "DELETE FROM year_value_bit WHERE year=? AND node=?",
                    (year, node),
                )
            else:
                conn.execute(
                    "UPDATE year_value_bit SET cnt=? WHERE year=? AND node=?",
                    (new_cnt, year, node),
                )
        if metrics is not None:
            metrics.bit_rows_written += 1
        node += node & -node


def _year_stat_change(
    conn: sqlite3.Connection, year: int, tree_delta: int, metrics: WriteMetrics | None
) -> None:
    """Adjust the year's tree count, dropping the row when the year empties."""
    row = conn.execute(
        "SELECT tree_count FROM year_stat WHERE year=?", (year,)
    ).fetchone()
    if tree_delta > 0:
        if row is None:
            conn.execute(
                "INSERT INTO year_stat(year, tree_count) VALUES(?,?)",
                (year, tree_delta),
            )
        else:
            conn.execute(
                "UPDATE year_stat SET tree_count=tree_count+? WHERE year=?",
                (tree_delta, year),
            )
    else:
        if row is None:  # pragma: no cover - defensive invariant
            raise sqlite3.DatabaseError(f"year_stat missing for year {year}")
        new_count = int(row["tree_count"]) + tree_delta
        if new_count == 0:
            # The whole year disappears: no stale aggregate may remain.
            conn.execute("DELETE FROM year_stat WHERE year=?", (year,))
            leftover = conn.execute(
                "SELECT COUNT(*) AS n FROM year_value_bit WHERE year=?", (year,)
            ).fetchone()
            if int(leftover["n"]) != 0:  # pragma: no cover - defensive invariant
                raise sqlite3.DatabaseError(f"Fenwick nodes left behind for year {year}")
        else:
            conn.execute(
                "UPDATE year_stat SET tree_count=? WHERE year=?",
                (new_count, year),
            )
    if metrics is not None:
        metrics.year_stat_rows_written += 1


def _maintain_year(
    conn: sqlite3.Connection,
    year: int,
    old_x2: int | None,
    new_x2: int | None,
    metrics: WriteMetrics | None,
) -> None:
    """Apply one tree's old -> new within-tree median change for a year.

    ``None`` means the tree has no core value for that year (it is not, or no
    longer, part of the year's sample). Every branch is O(BITS) Fenwick node
    touches plus one ``year_stat`` write; the year's other trees are never read.
    """
    if old_x2 == new_x2:
        return
    if metrics is not None:
        metrics.bit_median_changes += 1
    if old_x2 is None:
        # The median value appears in this year for the first time (or the
        # tree newly joins the year).
        _bit_add(conn, year, new_x2, +1, metrics)
        _year_stat_change(conn, year, +1, metrics)
    elif new_x2 is None:
        # The tree leaves the year; the value may disappear completely.
        _bit_add(conn, year, old_x2, -1, metrics)
        _year_stat_change(conn, year, -1, metrics)
    else:
        # Same tree stays in the year but its median value moves.
        _bit_add(conn, year, old_x2, -1, metrics)
        _bit_add(conn, year, new_x2, +1, metrics)


def _rebuild_tree_year(
    conn: sqlite3.Connection, tree_no: int, year: int, metrics: WriteMetrics | None
) -> int | None:
    """Recompute one (tree, year) median from that tree's own measurements.

    Only the tree's cores in this one year are scanned; the cost depends on
    the tree's core count, not on the year's sample-tree count. Returns the
    new doubled median, or ``None`` when the tree no longer has data in the
    year (its ``tree_year`` row is deleted).
    """
    rows = conn.execute(
        "SELECT index_value FROM measurement "
        "WHERE tree_no=? AND year=? ORDER BY index_value",
        (tree_no, year),
    ).fetchall()
    if metrics is not None:
        metrics.measurement_rows_scanned += len(rows)
    if not rows:
        conn.execute(
            "DELETE FROM tree_year WHERE year=? AND tree_no=?", (year, tree_no)
        )
        return None
    values = [int(r["index_value"]) for r in rows]
    median = _median_x2(values)
    if median > MAX_MEDIAN_X2:  # pragma: no cover - guarded by model validation
        raise ValueError(f"median_x2 {median} outside Fenwick domain")
    conn.execute(
        "INSERT INTO tree_year(year, tree_no, median_x2, core_count) VALUES(?,?,?,?) "
        "ON CONFLICT(year, tree_no) DO UPDATE SET "
        "median_x2=excluded.median_x2, core_count=excluded.core_count",
        (year, tree_no, median, len(values)),
    )
    if metrics is not None:
        metrics.tree_year_pairs_recomputed += 1
    return median


# ---------------------------------------------------------------------------
# Legacy volume migration (aggregate-only, single transaction)
# ---------------------------------------------------------------------------


def _migrate_legacy_schema(conn: sqlite3.Connection) -> None:
    """Upgrade a volume built by the cumulative-histogram schema.

    Only the already-committed aggregate tables (``tree_year`` and the legacy
    ``year_value_cum`` histogram) are read -- raw measurements are never
    rescanned. The whole migration runs in the caller's write transaction, so
    any failure rolls back and the legacy volume, its data and its
    ``data_version`` are preserved untouched.
    """
    if not table_exists(conn, LEGACY_HISTOGRAM_TABLE):
        return
    if table_exists(conn, BIT_TABLE) and conn.execute(
        "SELECT COUNT(*) AS n FROM year_value_bit"
    ).fetchone()["n"] > 0:
        raise sqlite3.DatabaseError(
            "cannot migrate: both legacy histogram and populated Fenwick table exist"
        )

    conn.execute(
        "CREATE TABLE IF NOT EXISTS year_value_bit ("
        "year INTEGER NOT NULL CHECK (year BETWEEN 1 AND 9999), "
        "node INTEGER NOT NULL CHECK (node >= 1), "
        "cnt INTEGER NOT NULL CHECK (cnt >= 1), "
        "PRIMARY KEY (year, node)) WITHOUT ROWID"
    )

    # Legacy year_stat carries an extra slot_count column; rebuild it in the
    # current shape instead of altering it.
    conn.execute(
        "CREATE TABLE year_stat_new ("
        "year INTEGER PRIMARY KEY CHECK (year BETWEEN 1 AND 9999), "
        "tree_count INTEGER NOT NULL CHECK (tree_count >= 1)) WITHOUT ROWID"
    )
    conn.execute("INSERT INTO year_stat_new(year, tree_count) SELECT year, tree_count FROM year_stat")
    conn.execute("DROP TABLE year_stat")
    conn.execute("ALTER TABLE year_stat_new RENAME TO year_stat")

    # Replay the legacy histogram (one row per distinct doubled median) into
    # the Fenwick tree. This is a one-time O(distinct values * BITS) rebuild.
    legacy_rows = conn.execute(
        "SELECT year, value_x2, cnt FROM year_value_cum ORDER BY year, value_x2"
    ).fetchall()
    for row in legacy_rows:
        _bit_add(
            conn,
            int(row["year"]),
            int(row["value_x2"]),
            int(row["cnt"]),
            None,
        )

    # The Fenwick root node (index = BIT_DOMAIN) covers the whole domain, so
    # its count must equal the committed tree count for every migrated year.
    bad = conn.execute(
        "SELECT s.year FROM year_stat s "
        "LEFT JOIN year_value_bit b ON b.year = s.year AND b.node = ? "
        "WHERE COALESCE(b.cnt, 0) != s.tree_count",
        (BIT_DOMAIN,),
    ).fetchall()
    if bad:
        raise sqlite3.DatabaseError("legacy migration consistency check failed")

    conn.execute(f"DROP TABLE {LEGACY_HISTOGRAM_TABLE}")
    _set_meta_int(conn, "schema_version", SCHEMA_VERSION)


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


def _apply_report_tx(
    conn: sqlite3.Connection, request: ReportRequest, metrics: WriteMetrics | None
) -> dict[str, int]:
    # An old data volume is upgraded before any new write is applied; the
    # migration and the report commit (or roll back) together.
    if table_exists(conn, LEGACY_HISTOGRAM_TABLE):
        _migrate_legacy_schema(conn)

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_version = _meta_int(conn, "data_version") + 1

    touched_trees: set[int] = set()
    tree_years: dict[int, set[int]] = {}
    affected_years: set[int] = set()
    measurements_written = 0

    for core in request.cores:
        touched_trees.add(core.tree_no)
        old_years = {
            int(r["year"])
            for r in conn.execute(
                "SELECT year FROM measurement WHERE tree_no=? AND core_no=?",
                (core.tree_no, core.core_no),
            ).fetchall()
        }
        years_for_tree = tree_years.setdefault(core.tree_no, set())
        years_for_tree.update(old_years)
        years_for_tree.update(core.years.keys())
        affected_years.update(old_years)
        affected_years.update(core.years.keys())

        # Upsert the core row first so the measurements can reference it.
        row = conn.execute(
            "SELECT revision FROM core WHERE tree_no=? AND core_no=?",
            (core.tree_no, core.core_no),
        ).fetchone()
        revision = (int(row["revision"]) + 1) if row is not None else 1
        conn.execute(
            "INSERT INTO core(tree_no, core_no, revision, updated_at, data_version) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(tree_no, core_no) DO UPDATE SET "
            "revision=excluded.revision, updated_at=excluded.updated_at, "
            "data_version=excluded.data_version",
            (core.tree_no, core.core_no, revision, now, new_version),
        )

        # Whole-series replacement: the old series is discarded, including
        # years that the new report omits.
        conn.execute(
            "DELETE FROM measurement WHERE tree_no=? AND core_no=?",
            (core.tree_no, core.core_no),
        )
        conn.executemany(
            "INSERT INTO measurement(tree_no, core_no, year, index_value) "
            "VALUES(?,?,?,?)",
            [
                (core.tree_no, core.core_no, year, idx)
                for year, idx in sorted(core.years.items())
            ],
        )
        measurements_written += len(core.years)

    # Capture the previously committed per-tree medians before any aggregate
    # row is overwritten; each lookup is a primary-key seek.
    old_medians: dict[tuple[int, int], int] = {}
    for tree_no in sorted(touched_trees):
        for year in sorted(tree_years[tree_no]):
            old_row = conn.execute(
                "SELECT median_x2 FROM tree_year WHERE year=? AND tree_no=?",
                (year, tree_no),
            ).fetchone()
            if metrics is not None:
                metrics.tree_year_rows_scanned += 1
            if old_row is not None:
                old_medians[(tree_no, year)] = int(old_row["median_x2"])

    # Recompute only the touched (tree, year) medians, then apply each old ->
    # new difference to that year's Fenwick tree. No other tree is read and no
    # whole-year aggregate is rebuilt.
    for tree_no in sorted(touched_trees):
        for year in sorted(tree_years[tree_no]):
            new_x2 = _rebuild_tree_year(conn, tree_no, year, metrics)
            old_x2 = old_medians.get((tree_no, year))
            _maintain_year(conn, year, old_x2, new_x2, metrics)

    _set_meta_int(conn, "data_version", new_version)

    return {
        "data_version": new_version,
        "trees_written": len(touched_trees),
        "cores_written": len(request.cores),
        "years_changed": len(affected_years),
        "measurements_written": measurements_written,
    }


def apply_report(
    db_path: str, request: ReportRequest, metrics: WriteMetrics | None = None
) -> dict[str, int]:
    """Apply one report atomically. Any failure rolls the whole report back.

    A legacy volume is migrated inside the same transaction before the report
    is applied, so a migration failure preserves the committed data and
    version unchanged.
    """

    def _run() -> dict[str, int]:
        conn = connect_write(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = _apply_report_tx(conn, request, metrics)
            conn.execute("COMMIT")
            return result
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    return busy_retry(_run)


def migrate_if_legacy(db_path: str) -> bool:
    """Upgrade a legacy volume once, before the service starts serving.

    Returns ``True`` when a migration was performed. The upgrade reads only
    the committed aggregate tables and commits in its own transaction; on any
    failure it rolls back, leaving the legacy volume, its data and its
    ``data_version`` untouched.
    """

    def _run() -> bool:
        conn = connect_write(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            if not table_exists(conn, LEGACY_HISTOGRAM_TABLE):
                conn.execute("ROLLBACK")
                return False
            _migrate_legacy_schema(conn)
            conn.execute("COMMIT")
            return True
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    return busy_retry(_run)


# ---------------------------------------------------------------------------
# Query path: order statistics on the sparse Fenwick tree
# ---------------------------------------------------------------------------


def _bit_kth_value_x2(
    session: ReadSession,
    year: int,
    k: int,
    excluded_value_x2: int | None,
) -> int:
    """Return the doubled median value holding the k-th smallest rank.

    Canonical Fenwick binary lifting. The running index ``idx`` only moves by
    the current mask, so every probed node is a valid Fenwick boundary; when a
    node's (possibly exclusion-corrected) count is strictly below the
    remaining rank ``k``, the whole block is skipped and its count is
    subtracted from ``k``. At most ``BITS`` primary-key probes.

    When one tree is excluded its median is subtracted on the fly from every
    probed node whose covered range contains that value's index -- nothing
    excluded is ever stored.
    """
    idx = 0
    excluded_index = None if excluded_value_x2 is None else excluded_value_x2 + 1
    mask = 1 << (BITS - 1)
    while mask:
        nxt = idx + mask
        session.metrics.slot_probes += 1
        row = session.fetchone(
            "SELECT cnt FROM year_value_bit WHERE year=? AND node=?",
            (year, nxt),
        )
        node_cnt = int(row["cnt"]) if row is not None else 0
        if excluded_index is not None and idx < excluded_index <= nxt:
            node_cnt -= 1
        if node_cnt < k:
            idx = nxt
            k -= node_cnt
        mask >>= 1
    # idx is the largest value index whose prefix count is below the rank, so
    # the selected 1-based value index is idx + 1 and the doubled value is idx.
    return idx


def query_reference(db_path: str, year: int, exclude_tree: int | None) -> dict:
    """Compute one year's reference value, optionally excluding one tree."""
    with read_snapshot(db_path, restricted=True) as session:
        # Defensive: a read against a still-legacy volume would silently see an
        # empty Fenwick tree and return nonsense. Startup normally migrates
        # first; refuse rather than return a wrong reference value.
        if table_exists(session.conn, LEGACY_HISTOGRAM_TABLE):
            raise sqlite3.DatabaseError(
                "database uses the legacy cumulative-histogram schema and has not been migrated"
            )
        stat = session.fetchone(
            "SELECT tree_count FROM year_stat WHERE year=?",
            (year,),
        )
        tree_count_in_year = int(stat["tree_count"]) if stat is not None else 0

        excluded_value_x2: int | None = None
        excluded_present = False
        if exclude_tree is not None:
            tree_row = session.fetchone(
                "SELECT median_x2 FROM tree_year WHERE year=? AND tree_no=?",
                (year, exclude_tree),
            )
            if tree_row is not None:
                excluded_present = True
                excluded_value_x2 = int(tree_row["median_x2"])

        sample_trees = tree_count_in_year - (1 if excluded_present else 0)
        metrics = session.metrics

        value_x4: int | None = None
        value: float | None = None
        enough = sample_trees >= MIN_TREES and tree_count_in_year > 0

        if enough:
            if sample_trees % 2 == 1:
                k = sample_trees // 2 + 1
                v1 = _bit_kth_value_x2(session, year, k, excluded_value_x2)
                value_x4 = 2 * v1
            else:
                k_low = sample_trees // 2
                v1 = _bit_kth_value_x2(
                    session, year, k_low, excluded_value_x2
                )
                v2 = _bit_kth_value_x2(
                    session, year, k_low + 1, excluded_value_x2
                )
                # v1/v2 are doubled tree medians; (v1+v2) is the reference
                # value in quadruple units: median = (v1+v2)/4 exactly.
                value_x4 = v1 + v2
            value = value_x4 / 4.0

        version = session.fetchone(
            "SELECT value FROM meta WHERE key='data_version'"
        )
        data_version = int(version["value"]) if version is not None else 0

    return {
        "year": year,
        "exclude_tree": exclude_tree,
        "sample_trees": sample_trees,
        "trees_in_year": tree_count_in_year,
        "enough_samples": enough,
        "status": "ok" if enough else "insufficient_samples",
        "min_trees": MIN_TREES,
        "value_x4": value_x4,
        "value": value,
        "excluded_tree_present": excluded_present,
        "excluded_tree_median_x2": excluded_value_x2,
        "data_version": data_version,
        "read_cost": metrics.as_dict(),
    }


# ---------------------------------------------------------------------------
# Administrative read helpers
# ---------------------------------------------------------------------------


def list_trees(db_path: str) -> list[int]:
    with read_snapshot(db_path, restricted=False) as session:
        rows = session.fetchall("SELECT DISTINCT tree_no FROM core ORDER BY tree_no")
        return [int(r["tree_no"]) for r in rows]


def get_core_series(db_path: str, tree_no: int, core_no: int) -> dict | None:
    with read_snapshot(db_path, restricted=False) as session:
        core = session.fetchone(
            "SELECT tree_no, core_no, revision, updated_at FROM core "
            "WHERE tree_no=? AND core_no=?",
            (tree_no, core_no),
        )
        if core is None:
            return None
        rows = session.fetchall(
            "SELECT year, index_value FROM measurement "
            "WHERE tree_no=? AND core_no=? ORDER BY year",
            (tree_no, core_no),
        )
        return {
            "tree_no": int(core["tree_no"]),
            "core_no": int(core["core_no"]),
            "revision": int(core["revision"]),
            "updated_at": core["updated_at"],
            "years": {int(r["year"]): int(r["index_value"]) for r in rows},
        }
