"""Dependency-graph validation, replacement and fault classification."""
from __future__ import annotations

from collections import deque

from . import phase_service
from .classification import classify_snapshot, load_graph
from .database import read_tx, write_tx
from .errors import GraphError
from .schemas import GraphPayload, GraphResponse, ServiceIn

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_graph(payload: GraphPayload) -> tuple[list[str], dict[str, list[str]]]:
    """Validate a full-graph payload.

    Returns ``(nodes, upstream)`` where ``upstream[s]`` is the sorted list of
    services ``s`` depends on.  Raises :class:`GraphError` on any structural
    problem (duplicate names, unknown references or cycles); the caller keeps
    the previously committed graph untouched in that case.
    """
    nodes: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    for svc in payload.services:
        if svc.name in seen:
            duplicates.append(svc.name)
        seen.add(svc.name)
        nodes.append(svc.name)
    if duplicates:
        raise GraphError(f"duplicate service names: {sorted(set(duplicates))}")

    node_set = set(nodes)
    upstream: dict[str, list[str]] = {}
    unknown: set[tuple[str, str]] = set()
    dup_edges: set[tuple[str, str]] = set()

    for svc in payload.services:
        deps: list[str] = []
        edge_seen: set[str] = set()
        for dep in svc.dependencies:
            if dep == svc.name:
                raise GraphError(f"service {svc.name!r} cannot depend on itself")
            if dep not in node_set:
                unknown.add((svc.name, dep))
            if dep in edge_seen:
                dup_edges.add((svc.name, dep))
            edge_seen.add(dep)
            deps.append(dep)
        upstream[svc.name] = deps

    if unknown:
        detail = ", ".join(f"{a}->{b}" for a, b in sorted(unknown))
        raise GraphError(f"unknown service references: {detail}")
    if dup_edges:
        detail = ", ".join(f"{a}->{b}" for a, b in sorted(dup_edges))
        raise GraphError(f"duplicate dependency edges: {detail}")

    _ensure_acyclic(nodes, upstream)
    return nodes, upstream


def _ensure_acyclic(nodes: list[str], upstream: dict[str, list[str]]) -> None:
    """Kahn's algorithm; anything left over when the queue drains is on a cycle."""
    indegree = {n: 0 for n in nodes}
    dependents: dict[str, list[str]] = {n: [] for n in nodes}
    for downstream, deps in upstream.items():
        for up in deps:
            indegree[downstream] += 1
            dependents[up].append(downstream)

    queue = deque(sorted(n for n, d in indegree.items() if d == 0))
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for child in sorted(dependents[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if visited != len(nodes):
        cyclic = sorted(n for n, d in indegree.items() if d > 0)
        raise GraphError(f"dependency cycle detected involving: {cyclic}")


# ---------------------------------------------------------------------------
# Replacement
# ---------------------------------------------------------------------------


def replace_graph(payload: GraphPayload) -> dict:
    """Atomically replace the whole graph.

    Service-state rows (up/down + last seq) are keyed by service name and live
    outside the graph tables, so states of same-named services survive a
    replacement.  The swap of nodes/edges/meta happens in one write
    transaction and is therefore all-or-nothing.
    """
    nodes, upstream = validate_graph(payload)

    with write_tx() as conn:
        row = conn.execute("SELECT revision FROM graph_meta WHERE id = 1").fetchone()
        new_revision = (row["revision"] + 1) if row else 1

        conn.execute("DELETE FROM edges")
        conn.execute("DELETE FROM nodes")
        conn.executemany(
            "INSERT INTO nodes (service, revision) VALUES (?, ?)",
            [(n, new_revision) for n in nodes],
        )
        edge_rows = [
            (new_revision, up, downstream)
            for downstream, deps in upstream.items()
            for up in deps
        ]
        conn.executemany(
            "INSERT INTO edges (revision, upstream, downstream) VALUES (?, ?, ?)",
            edge_rows,
        )
        conn.execute(
            "INSERT INTO graph_meta (id, revision, version) VALUES (1, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET revision = excluded.revision, "
            "version = excluded.version, "
            "committed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
            (new_revision, payload.version),
        )
        # The swap can change classification without any new report (e.g. a
        # dropped upstream edge exposes a suppressed fault).  Phases are synced
        # in this same transaction: a service that stays actionable keeps its
        # phase and acknowledgement, a newly actionable one gets a fresh phase.
        phase_service.sync_alert_phases(conn)
        preserved = [
            r["service"]
            for r in conn.execute(
                "SELECT service FROM service_state WHERE service IN (%s) ORDER BY service"
                % ",".join("?" * len(nodes)),
                nodes,
            )
        ]
    return {
        "accepted": True,
        "revision": new_revision,
        "version": payload.version,
        "service_count": len(nodes),
        "edge_count": len(edge_rows),
        "preserved_state": preserved,
    }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def get_graph() -> GraphResponse:
    with read_tx() as conn:
        revision, version, upstream = load_graph(conn)
        meta = conn.execute(
            "SELECT committed_at FROM graph_meta WHERE id = 1"
        ).fetchone()
        services = [
            ServiceIn(name=name, dependencies=sorted(deps))
            for name, deps in sorted(upstream.items())
        ]
    return GraphResponse(
        revision=revision,
        version=version,
        committed_at=meta["committed_at"],
        services=services,
    )


# ---------------------------------------------------------------------------
# Fault classification
# ---------------------------------------------------------------------------


def classify_alerts() -> dict:
    """Return the alert classification plus each actionable alert's phase.

    Classification and phase rows are read from one committed snapshot.  Phase
    numbers are allocated by write transactions (reports / graph swaps) and
    backfilled by :func:`backfill_phases` when a legacy database is opened.
    """
    with read_tx() as conn:
        snapshot = classify_snapshot(conn)
        phases = phase_service.load_phases(conn)

    for fault in snapshot["actionable_faults"]:
        row = phases.get(fault["service"])
        # A missing row can only mean a database written before phase tracking
        # existed and not yet backfilled; expose it as an unacknowledged phase 0
        # which no acknowledgement can match.
        fault["phase"] = row["phase"] if row else 0
        fault["acknowledged"] = bool(row and row["acknowledged_at"])
        fault["acknowledged_at"] = row["acknowledged_at"] if row else None

    return snapshot


def backfill_phases() -> None:
    """Initialise phase rows for a database committed before this feature.

    Safe to call on every startup: it only writes when classification says a
    service is actionable but no phase row exists yet.  If no graph has been
    committed there is nothing to do.
    """
    from .classification import actionable_services
    from .errors import NoGraph

    try:
        with write_tx() as conn:
            phases = phase_service.load_phases(conn)
            actionable = actionable_services(conn)
            if any(svc not in phases for svc in actionable):
                phase_service.sync_alert_phases(conn)
    except NoGraph:
        pass
