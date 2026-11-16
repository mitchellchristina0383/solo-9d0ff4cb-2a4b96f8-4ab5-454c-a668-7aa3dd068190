"""Transaction-level tests for alert phase/acknowledgement atomicity.

These exercise the service layer directly (no HTTP) against a temporary
SQLite file.  They cover the invariant that a failing write must not advance
phases, change alert state, or rewrite an acknowledgement, and that phase
allocation is atomic with the report/graph swap that causes it.

Run:
    python -I tests/test_phase_atomicity.py
"""
from __future__ import annotations

import os
import sys
import tempfile

# Never touch the production database from a unit test.
_TMP = tempfile.mkdtemp(prefix="alert-phase-")
os.environ["ALERT_DB_PATH"] = os.path.join(_TMP, "alerts.db")

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# ``-I`` ignores PYTHONPATH, so make the vendored dependencies importable
# explicitly (the Docker image installs them system-wide, making this a no-op).
_VENDORED = os.path.join(_ROOT, ".pylibs")
for path in (_ROOT, _VENDORED):
    if path not in sys.path:
        sys.path.insert(0, path)

from app import phase_service  # noqa: E402
from app.database import init_db, read_tx, write_tx  # noqa: E402
from app.errors import ReportConflict, StalePhase  # noqa: E402
from app.graph_service import (  # noqa: E402
    backfill_phases,
    classify_alerts,
    replace_graph,
)
from app.report_service import submit_report  # noqa: E402
from app.schemas import GraphPayload, ReportPayload  # noqa: E402

PASS = 0
FAIL = 0
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        FAILURES.append(f"{name} {detail}")
        print(f"  FAIL  {name} {detail}")


def reset() -> None:
    init_db()
    with write_tx() as conn:
        for table in ("events", "alert_phase", "service_state", "edges", "nodes", "graph_meta"):
            conn.execute(f"DELETE FROM {table}")


def graph(*services: tuple[str, list[str]]) -> None:
    replace_graph(
        GraphPayload(
            version="t",
            services=[
                {"name": name, "dependencies": deps} for name, deps in services
            ],
        )
    )


def fault(service: str, seq: int) -> dict:
    return submit_report(ReportPayload(service=service, seq=seq, status="fault"))


def recover(service: str, seq: int) -> dict:
    return submit_report(ReportPayload(service=service, seq=seq, status="recover"))


def ack(service: str, phase: int) -> dict:
    return phase_service.record_acknowledgement(service, phase)


def phase_of(service: str) -> dict:
    for f in classify_alerts()["actionable_faults"]:
        if f["service"] == service:
            return f
    return {}


def state_rows() -> dict[str, tuple[str, int]]:
    with read_tx() as conn:
        return {
            r["service"]: (r["status"], r["last_seq"])
            for r in conn.execute("SELECT service, status, last_seq FROM service_state")
        }


def phase_rows() -> dict[str, tuple[int, int, str | None]]:
    with read_tx() as conn:
        return {
            r["service"]: (r["phase"], r["actionable"], r["acknowledged_at"])
            for r in conn.execute(
                "SELECT service, phase, actionable, acknowledged_at FROM alert_phase"
            )
        }


def test_failed_report_write_is_atomic() -> None:
    print("[T1] a failing report transaction leaves state and phases untouched")
    reset()
    graph(("A", []), ("B", []))
    fault("A", 1)
    ack("A", 1)
    before_state = state_rows()
    before_phase = phase_rows()
    snapshot = classify_alerts()

    original = phase_service.sync_alert_phases

    def boom(conn):
        # Simulate a failure after the report row and state were written but
        # before commit.
        raise RuntimeError("injected write failure")

    phase_service.sync_alert_phases = boom
    try:
        # Recover A: if the transaction were not atomic, A's state would be up
        # and its phase inactive despite the error.
        try:
            recover("A", 2)
            raised = False
        except RuntimeError:
            raised = True
        finally:
            phase_service.sync_alert_phases = original
    finally:
        phase_service.sync_alert_phases = original

    check("injected failure surfaces to the caller", raised)
    check("failed report did not change service state", state_rows() == before_state,
          f"{before_state} -> {state_rows()}")
    check("failed report did not change phases", phase_rows() == before_phase,
          f"{before_phase} -> {phase_rows()}")
    check("failed report left the alert classification unchanged",
          classify_alerts() == snapshot, "classification drifted")
    check("acknowledgement survived the failed write",
          phase_of("A").get("acknowledged") is True, str(phase_of("A")))


def test_failed_graph_swap_is_atomic() -> None:
    print("[T2] a failing graph swap does not commit graph, state or phases")
    reset()
    graph(("A", ["B"]), ("B", []))
    fault("A", 1)
    fault("B", 1)
    # B suppresses A; acknowledge B.
    ack("B", 1)
    before_graph = replace_graph(
        GraphPayload(version="probe", services=[
            {"name": "A", "dependencies": ["B"]},
            {"name": "B", "dependencies": []},
        ])
    )
    before_phase = phase_rows()
    before_snapshot = classify_alerts()

    original = phase_service.sync_alert_phases

    def boom(conn):
        raise RuntimeError("injected graph write failure")

    phase_service.sync_alert_phases = boom
    try:
        try:
            # Removing the B->A edge would make A actionable and allocate a
            # new phase; the injected failure must roll all of that back.
            graph(("A", []), ("B", []))
            raised = False
        except RuntimeError:
            raised = True
    finally:
        phase_service.sync_alert_phases = original

    check("injected graph failure surfaces to the caller", raised)
    after_graph = replace_graph(
        GraphPayload(version="probe2", services=[
            {"name": "A", "dependencies": ["B"]},
            {"name": "B", "dependencies": []},
        ])
    )
    check("failed swap did not commit a new revision",
          after_graph["revision"] == before_graph["revision"] + 1,
          f"{before_graph} -> {after_graph}")
    # The rollback means the old adjacency (A->B) was still in place when the
    # probe swap ran, so A must still be suppressed and B still acknowledged.
    check("failed swap did not change phases", phase_rows()["B"][2] is not None,
          str(phase_rows()))
    check("failed swap left A suppressed", not phase_of("A"), str(classify_alerts()))
    check("failed swap left B acknowledged", phase_of("B").get("acknowledged") is True,
          str(phase_of("B")))
    check("failed swap left classification unchanged",
          {f["service"] for f in classify_alerts()["actionable_faults"]}
          == {f["service"] for f in before_snapshot["actionable_faults"]},
          f"{before_snapshot} -> {classify_alerts()}")


def test_failed_ack_write_is_atomic() -> None:
    print("[T3] a failing acknowledgement write leaves state and phases untouched")
    reset()
    graph(("A", []))
    fault("A", 1)
    ack("A", 1)
    before = phase_rows()

    def boom(conn, actionable):
        # Fail after the phase sync but before the ack timestamp write is
        # committed: the previously recorded ack must remain intact.
        raise RuntimeError("injected ack failure")

    # Patch sync_phases, which record_acknowledgement runs inside its write
    # transaction before validating and writing the acknowledgement.
    original = phase_service.sync_phases
    phase_service.sync_phases = boom
    try:
        try:
            ack("A", 1)
            raised = False
        except RuntimeError:
            raised = True
    finally:
        phase_service.sync_phases = original

    check("injected ack failure surfaces", raised)
    check("failed ack did not rewrite the acknowledgement timestamp",
          phase_rows() == before, f"{before} -> {phase_rows()}")
    check("failed ack left the phase actionable and acknowledged",
          phase_of("A").get("phase") == 1 and phase_of("A").get("acknowledged") is True,
          str(phase_of("A")))


def test_stale_ack_is_rejected_without_write() -> None:
    print("[T4] stale acknowledgements are rejected and write nothing")
    reset()
    graph(("A", ["B"]), ("B", []))
    fault("A", 1)
    ack("A", 1)
    fault("B", 1)  # suppresses A
    before = phase_rows()

    try:
        ack("A", 1)
        rejected = False
    except StalePhase:
        rejected = True
    check("ack while suppressed rejected", rejected)
    check("rejected ack wrote nothing", phase_rows() == before,
          f"{before} -> {phase_rows()}")

    recover("B", 2)  # A actionable again -> new phase
    a = phase_of("A")
    check("A re-entered with a new phase", a.get("phase") == 2, str(a))
    try:
        ack("A", 1)
        rejected = False
    except StalePhase:
        rejected = True
    check("old phase ack rejected after suppression ends", rejected)
    check("rejected stale ack did not write an acknowledgement",
          phase_of("A").get("acknowledged") is False, str(phase_of("A")))


def test_legacy_database_backfill() -> None:
    print("[T5] legacy database (no phase rows) is backfilled at startup")
    reset()
    graph(("A", []), ("B", ["A"]))
    fault("A", 1)
    # Simulate a database written before phase tracking existed.
    with write_tx() as conn:
        conn.execute("DELETE FROM alert_phase")
    check("no phase rows before backfill", phase_rows() == {}, str(phase_rows()))
    check("alerts expose a non-acknowledgeable phase 0 for legacy rows",
          phase_of("A").get("phase") == 0, str(phase_of("A")))
    try:
        ack("A", 0)
        rejected = False
    except Exception:
        rejected = True
    check("legacy phase 0 cannot be acknowledged", rejected)

    backfill_phases()
    phases = phase_rows()
    check("backfill created a phase for the actionable service",
          phases.get("A", (None,))[0] == 1, str(phases))
    check("backfill leaves the acknowledged flag unset",
          phase_of("A").get("acknowledged") is False, str(phase_of("A")))
    ack("A", 1)
    check("backfilled phase can be acknowledged",
          phase_of("A").get("acknowledged") is True, str(phase_of("A")))

    # Running startup backfill again is a no-op and keeps the phase/ack.
    backfill_phases()
    check("second backfill keeps phase and acknowledgement",
          phase_of("A").get("phase") == 1 and phase_of("A").get("acknowledged") is True,
          str(phase_of("A")))


def test_duplicate_report_does_not_touch_phase() -> None:
    print("[T6] idempotent retries and same-classification swaps keep the ack")
    reset()
    graph(("A", ["B"]), ("B", []))
    fault("A", 1)
    fault("B", 1)
    ack("B", 1)
    before = phase_rows()

    # Same (service, seq, content): idempotent, no phase sync at all.
    out = fault("B", 1)
    check("same-seq retry is idempotent", out["duplicate"] is True, str(out))
    check("idempotent retry leaves phases untouched", phase_rows() == before,
          f"{before} -> {phase_rows()}")

    # Conflicting/stale reports are rejected before any phase sync.
    try:
        recover("B", 1)
        rejected = False
    except ReportConflict:
        rejected = True
    check("same-seq different content rejected", rejected)
    try:
        fault("B", 1)
        rejected = False
    except ReportConflict:
        # Earlier fault("B",1) was applied; retrying same seq fault is a
        # duplicate, so this branch should not be taken.
        rejected = True
    check("same-seq same-content fault retry not a conflict", not rejected)

    # A higher-seq fault that leaves B actionable must keep phase and ack.
    fault("B", 2)
    check("applied fault with unchanged classification keeps phase/ack",
          phase_rows() == before, f"{before} -> {phase_rows()}")

    # Swap that preserves classification keeps phase/ack.
    graph(("A", ["B"]), ("B", []))
    check("classification-unchanged swap keeps phase/ack", phase_rows() == before,
          f"{before} -> {phase_rows()}")
    # A was briefly actionable before B reported, so it has an ended phase row;
    # it must not be actionable or acknowledged while suppressed.
    check("suppressed A has no current actionable phase",
          phase_rows()["A"][1] == 0 and phase_rows()["A"][2] is None,
          str(phase_rows()))


def main() -> int:
    test_failed_report_write_is_atomic()
    test_failed_graph_swap_is_atomic()
    test_failed_ack_write_is_atomic()
    test_stale_ack_is_rejected_without_write()
    test_legacy_database_backfill()
    test_duplicate_report_does_not_touch_phase()

    print(f"\n{'-' * 60}\nPASS={PASS} FAIL={FAIL}")
    if FAIL:
        print("Failures:")
        for f in FAILURES:
            print(" -", f)
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
