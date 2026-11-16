"""Consistent graph snapshot loading and alert classification.

Both alert queries and phase maintenance must classify from exactly the same
data they are about to mutate (or report).  These helpers therefore operate on
a caller-supplied connection inside the caller's read/write transaction.
"""
from __future__ import annotations

import sqlite3

from .errors import NoGraph


def load_graph(
    conn: sqlite3.Connection,
) -> tuple[int, str | None, dict[str, list[str]]]:
    """Load the committed graph revision and adjacency from ``conn``.

    ``upstream[s]`` is the sorted-by-query list of services ``s`` depends on.
    """
    meta = conn.execute(
        "SELECT revision, version FROM graph_meta WHERE id = 1"
    ).fetchone()
    if meta is None:
        raise NoGraph("no dependency graph has been committed yet")
    upstream: dict[str, list[str]] = {
        r["service"]: [] for r in conn.execute("SELECT service FROM nodes")
    }
    for r in conn.execute(
        "SELECT edges.upstream AS upstream, edges.downstream AS downstream "
        "FROM edges ORDER BY edges.upstream, edges.downstream"
    ):
        upstream[r["downstream"]].append(r["upstream"])
    return meta["revision"], meta["version"], upstream


def actionable_services(conn: sqlite3.Connection) -> set[str]:
    """Return the set of actionable down-services from ``conn``'s snapshot.

    Same semantics as :func:`classify_snapshot` but skips path enumeration, so
    it is cheap enough to run on every applied write.
    """
    _, _, upstream = load_graph(conn)
    down = {
        r["service"]
        for r in conn.execute(
            "SELECT service FROM service_state WHERE status = 'down'"
        )
    }
    down_here = {s for s in down if s in upstream}
    disc = suppressed_faults(down_here, upstream)
    return down_here - set(disc)


def classify_snapshot(conn: sqlite3.Connection) -> dict:
    """Compute actionable vs suppressed faults from one committed snapshot.

    A faulty service is *suppressed* only when there exists a dependency path
    on which **every** node is faulty leading to a root cause (a faulty
    service with no faulty upstream).  Healthy or never-reported nodes cut
    paths, so a fault behind such a node stays actionable.
    """
    revision, version, upstream = load_graph(conn)
    down: set[str] = {
        r["service"]
        for r in conn.execute(
            "SELECT service FROM service_state WHERE status = 'down'"
        )
    }
    seq_rows = {
        r["service"]: r["last_seq"]
        for r in conn.execute("SELECT service, last_seq FROM service_state")
    }

    down_here = {s for s in down if s in upstream}
    suppressed_services = suppressed_faults(down_here, upstream)

    # All-all-down paths from a node up to a given root (for transparency).
    def all_down_paths(node: str, root: str) -> list[list[str]]:
        if node == root:
            return [[node]]
        paths: list[list[str]] = []
        for up in upstream.get(node, []):
            if up in down_here:
                for tail in all_down_paths(up, root):
                    paths.append([node] + tail)
        return paths

    actionable: list[dict] = []
    suppressed: list[dict] = []
    for svc in sorted(down_here):
        # A root cause (faulty node with no faulty upstream) is always
        # actionable; only non-root nodes can be suppressed.
        svc_roots = sorted(suppressed_services.get(svc, ()))
        if svc_roots:
            paths: list[list[str]] = []
            for root in svc_roots:
                paths.extend(all_down_paths(svc, root))
            suppressed.append(
                {
                    "service": svc,
                    "last_seq": seq_rows[svc],
                    "root_causes": svc_roots,
                    "paths": sorted(paths),
                }
            )
        else:
            actionable.append({"service": svc, "last_seq": seq_rows[svc]})

    all_nodes = set(upstream)
    unreported = sorted(all_nodes - set(seq_rows))
    healthy = sorted(s for s in seq_rows if s in all_nodes and s not in down_here)

    return {
        "graph_revision": revision,
        "graph_version": version,
        "services": sorted(all_nodes),
        "actionable_faults": actionable,
        "suppressed_faults": suppressed,
        "down_services": sorted(down_here),
        "unreported_services": unreported,
        "healthy_services": healthy,
    }


def suppressed_faults(
    down: set[str], upstream: dict[str, list[str]]
) -> dict[str, list[str]]:
    """Map each suppressed down-service to the sorted root causes suppressing it.

    Roots are down services with no down upstream.  A non-root down service is
    suppressed exactly when some all-down path reaches a root.  Computed with a
    memoized DFS (linear in nodes + edges) and no path enumeration, so the
    write path that only needs the actionable set stays cheap.
    """
    down_here = {s for s in down if s in upstream}
    roots = {s for s in down_here if not any(u in down_here for u in upstream[s])}

    memo: dict[str, set[str]] = {}

    def reachable_roots(node: str) -> set[str]:
        if node in memo:
            return memo[node]
        if node in roots:
            memo[node] = {node}
            return memo[node]
        found: set[str] = set()
        for up in upstream.get(node, []):
            if up in down_here:
                found |= reachable_roots(up)
        memo[node] = found
        return found

    return {
        svc: sorted(reachable_roots(svc))
        for svc in down_here
        if svc not in roots and reachable_roots(svc)
    }
