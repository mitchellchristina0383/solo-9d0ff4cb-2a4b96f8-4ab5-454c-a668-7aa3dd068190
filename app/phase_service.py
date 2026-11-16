"""Alert phase tracking and handling acknowledgements.

An *alert phase* is one continuous period during which a service remains
actionable (i.e. appears in ``actionable_faults``).  A phase starts when a
service enters the actionable set and ends when it leaves it: recovery, being
suppressed by an upstream fault, or removal from the graph.  The next entry
allocates a new monotonically increasing phase number, so an acknowledgement
can only ever refer to the exact phase it was recorded against.

Phase rows are updated in the *same* SQLite transaction as the report or graph
replacement that changes the classification.  A failed write therefore rolls
back phase changes together with the state change, and a concurrent reader
always sees phases matching one committed state.
"""
from __future__ import annotations

import sqlite3

from .database import write_tx

_NOW = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"


def load_phases(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return ``service -> {phase, actionable, acknowledged_at}``."""
    return {
        row["service"]: {
            "phase": row["phase"],
            "actionable": bool(row["actionable"]),
            "acknowledged_at": row["acknowledged_at"],
        }
        for row in conn.execute(
            "SELECT service, phase, actionable, acknowledged_at FROM alert_phase"
        )
    }


def sync_alert_phases(conn: sqlite3.Connection) -> None:
    """Recompute the actionable set from ``conn`` and sync phases.

    Must run inside the write transaction that also contains the state or
    graph change being reflected, so phase transitions commit atomically with
    the data that caused them.  Uses the path-free computation: phases only
    depend on which services are actionable, not on the reported paths.
    """
    from .classification import actionable_services

    sync_phases(conn, actionable_services(conn))


def sync_phases(conn: sqlite3.Connection, actionable: set[str]) -> None:
    """Align stored phases with the actionable set of the current snapshot.

    * service newly actionable      -> phase + 1, acknowledgement cleared
    * service no longer actionable  -> marked inactive, acknowledgement cleared
    * service still actionable      -> untouched, so an idempotent report retry
      (same seq) or a graph swap that leaves the classification unchanged keeps
      the existing phase and acknowledgement.
    """
    rows = {
        row["service"]: row
        for row in conn.execute("SELECT service, phase, actionable FROM alert_phase")
    }

    # Leaving the actionable set ends the current episode.
    for service, row in rows.items():
        if service not in actionable and row["actionable"]:
            conn.execute(
                "UPDATE alert_phase SET actionable = 0, acknowledged_at = NULL, "
                f"updated_at = {_NOW} WHERE service = ?",
                (service,),
            )

    # (Re-)entering the actionable set always allocates a brand new phase,
    # even if a previous episode was acknowledged.
    for service in sorted(actionable):
        row = rows.get(service)
        if row is None:
            conn.execute(
                "INSERT INTO alert_phase (service, phase, actionable) VALUES (?, 1, 1)",
                (service,),
            )
        elif not row["actionable"]:
            conn.execute(
                "UPDATE alert_phase SET phase = phase + 1, actionable = 1, "
                f"acknowledged_at = NULL, updated_at = {_NOW} WHERE service = ?",
                (service,),
            )


def record_acknowledgement(service: str, phase: int) -> dict:
    """Acknowledge ``phase`` of ``service`` if it is still the current phase.

    The classification refresh, the phase/actionable check and the write all
    run in one ``BEGIN IMMEDIATE`` transaction, so an acknowledgement can
    never be attached to a phase that a concurrent report or graph replacement
    has already superseded.  Acknowledging the same current phase twice is
    idempotent (``duplicate=True``); the first timestamp is kept.
    """
    from .classification import actionable_services
    from .errors import NoGraph, StalePhase, UnknownService

    with write_tx() as conn:
        try:
            actionable = actionable_services(conn)
            graph_nodes = {
                r["service"] for r in conn.execute("SELECT service FROM nodes")
            }
        except NoGraph:
            raise UnknownService(
                f"service {service!r} is not part of the current graph"
            ) from None
        if service not in graph_nodes:
            raise UnknownService(
                f"service {service!r} is not part of the current graph"
            )

        # Defensive self-heal inside the same write transaction: if any phase
        # rows lag the committed classification (e.g. a database created before
        # phase tracking), align them first.  When classification is already
        # in sync this is a no-op and cannot invalidate a valid phase.
        sync_phases(conn, actionable)

        row = conn.execute(
            "SELECT phase, actionable, acknowledged_at FROM alert_phase "
            "WHERE service = ?",
            (service,),
        ).fetchone()
        if row is None or not row["actionable"] or row["phase"] != phase:
            if row is None or not row["actionable"]:
                reason = "the service has no actionable alert in the current state"
            else:
                reason = f"the current actionable phase is {row['phase']}"
            raise StalePhase(
                f"stale phase {phase} for service {service!r}: {reason}; "
                "refresh /alerts and acknowledge the current phase"
            )

        duplicate = row["acknowledged_at"] is not None
        if not duplicate:
            conn.execute(
                "UPDATE alert_phase SET acknowledged_at = "
                f"{_NOW}, updated_at = {_NOW} WHERE service = ?",
                (service,),
            )
        acknowledged_at = conn.execute(
            "SELECT acknowledged_at FROM alert_phase WHERE service = ?",
            (service,),
        ).fetchone()["acknowledged_at"]

    return {
        "acknowledged": True,
        "duplicate": duplicate,
        "service": service,
        "phase": phase,
        "acknowledged_at": acknowledged_at,
    }
