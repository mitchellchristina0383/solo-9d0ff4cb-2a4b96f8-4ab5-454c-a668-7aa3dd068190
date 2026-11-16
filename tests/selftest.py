"""End-to-end self-test for the Dependency Alert API.

Runs entirely on the Python standard library so it works inside the slim
Docker container as well as on a host.  Point it at a running server:

    python tests/selftest.py            # default http://127.0.0.1:8000
    BASE_URL=http://host:8000 python tests/selftest.py

Exits non-zero on any failed check.  Covers graph validation/replacement,
report sequence semantics, suppression rules, recovery reclassification and
key concurrent interleavings.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")

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


def request(method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode() or "{}")


def reset() -> None:
    status, _ = request("POST", "/admin/reset")
    assert status == 200, status


# ---------------------------------------------------------------------------
# Test graphs
# ---------------------------------------------------------------------------

# A -> B -> D, A -> C -> D, plus X -> Y  (edges stored as "downstream deps on upstream")
GRAPH_V1 = {
    "version": "v1",
    "services": [
        {"name": "A", "dependencies": ["B", "C"]},
        {"name": "B", "dependencies": ["D"]},
        {"name": "C", "dependencies": ["D"]},
        {"name": "D", "dependencies": []},
        {"name": "X", "dependencies": ["Y"]},
        {"name": "Y", "dependencies": []},
    ],
}

# v2: same A/B/C/D topology (states preserved), Y removed, Z added (X->Z)
GRAPH_V2 = {
    "version": "v2",
    "services": [
        {"name": "A", "dependencies": ["B", "C"]},
        {"name": "B", "dependencies": ["D"]},
        {"name": "C", "dependencies": ["D"]},
        {"name": "D", "dependencies": []},
        {"name": "X", "dependencies": ["Z"]},
        {"name": "Z", "dependencies": []},
    ],
}

CYCLIC_GRAPH = {
    "services": [
        {"name": "A", "dependencies": ["B"]},
        {"name": "B", "dependencies": ["C"]},
        {"name": "C", "dependencies": ["A"]},
    ]
}

UNKNOWN_REF_GRAPH_BAD = {
    "services": [{"name": "A", "dependencies": ["GHOST"]}]
}


def test_graph_validation() -> None:
    print("[T1] graph validation & atomic replacement")
    reset()

    status, body = request("GET", "/alerts")
    check("alerts before any graph -> 409", status == 409, str(body))

    status, body = request("POST", "/graph", CYCLIC_GRAPH)
    check("cyclic graph rejected 422", status == 422 and body["error"] == "invalid_graph", str(body))

    status, body = request("POST", "/graph", UNKNOWN_REF_GRAPH_BAD)
    check("unknown reference rejected 422", status == 422, str(body))

    status, body = request("POST", "/graph", {
        "services": [
            {"name": "A", "dependencies": ["B"]},
            {"name": "A", "dependencies": []},
            {"name": "B", "dependencies": []},
        ]
    })
    check("duplicate service rejected 422", status == 422, str(body))

    status, body = request("POST", "/graph", {
        "services": [
            {"name": "A", "dependencies": ["B", "B"]},
            {"name": "B", "dependencies": []},
        ]
    })
    check("duplicate edge rejected 422", status == 422, str(body))

    status, body = request("POST", "/graph", {
        "services": [{"name": "A", "dependencies": ["A"]}]
    })
    check("self dependency rejected 422", status == 422, str(body))

    status, body = request("POST", "/graph", GRAPH_V1)
    check("valid graph accepted", status == 200 and body["revision"] == 1, str(body))
    check("edge count correct", body["edge_count"] == 5, str(body))

    # Rejected replacement must keep the old version.
    status, _ = request("POST", "/graph", CYCLIC_GRAPH)
    status, body = request("GET", "/graph")
    check(
        "old graph retained after rejected replacement",
        status == 200 and body["revision"] == 1 and body["version"] == "v1",
        str(body),
    )

    status, body = request("POST", "/graph", GRAPH_V1)
    check("re-commit increments revision", status == 200 and body["revision"] == 2, str(body))


def test_report_semantics() -> None:
    print("[T2] report sequence semantics")
    reset()
    request("POST", "/graph", GRAPH_V1)

    status, body = request("POST", "/reports", {"service": "NOPE", "seq": 1, "status": "fault"})
    check("unknown service report -> 404", status == 404, str(body))

    status, body = request("POST", "/reports", {"service": "D", "seq": 0, "status": "fault"})
    check("non-positive seq -> 422", status == 422, str(body))

    status, body = request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    check("first fault applied", status == 200 and body["applied"] is True, str(body))

    status, body = request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    check(
        "same seq same content idempotent retry",
        status == 200 and body["applied"] is False and body["duplicate"] is True,
        str(body),
    )

    status, body = request("POST", "/reports", {"service": "D", "seq": 1, "status": "recover"})
    check("same seq different content -> 409", status == 409, str(body))

    # State must not have been changed by the conflicting retry.
    status, alerts = request("GET", "/alerts")
    d_down = "D" in alerts["down_services"]
    check("conflict did not change state", d_down, str(alerts))

    status, body = request("POST", "/reports", {"service": "D", "seq": 3, "status": "recover"})
    check("higher seq applies (gaps allowed)", status == 200 and body["applied"] is True, str(body))

    status, body = request("POST", "/reports", {"service": "D", "seq": 2, "status": "fault"})
    check("late lower seq -> 409", status == 409, str(body))

    status, alerts = request("GET", "/alerts")
    check("late report never rolls state back", "D" not in alerts["down_services"], str(alerts))

    status, body = request("POST", "/reports", {"service": "D", "seq": 3, "status": "fault"})
    check("retry of old seq after newer state -> 409", status == 409, str(body))


def test_suppression() -> None:
    print("[T3] suppression: only all-faulty paths suppress; healthy/unreported cut paths")
    reset()
    request("POST", "/graph", GRAPH_V1)

    def fault(svc: str, seq: int):
        return request("POST", "/reports", {"service": svc, "seq": seq, "status": "fault"})

    fault("D", 1)
    fault("B", 1)
    fault("A", 1)

    status, alerts = request("GET", "/alerts")
    actionable = [f["service"] for f in alerts["actionable_faults"]]
    suppressed = {f["service"]: f for f in alerts["suppressed_faults"]}
    check("root D is actionable", actionable == ["D"], str(alerts))
    check("B suppressed by D", "B" in suppressed and suppressed["B"]["root_causes"] == ["D"], str(alerts))
    check(
        "A suppressed only via all-down B-D path (C healthy cuts the other branch)",
        "A" in suppressed and suppressed["A"]["root_causes"] == ["D"],
        str(alerts),
    )
    check(
        "A path list only contains all-down paths",
        suppressed["A"]["paths"] == [["A", "B", "D"]],
        str(suppressed["A"]),
    )

    # C becomes faulty too: second all-down path opens.
    fault("C", 1)
    _, alerts = request("GET", "/alerts")
    suppressed = {f["service"]: f for f in alerts["suppressed_faults"]}
    check(
        "A gains second all-down path A-C-D",
        suppressed["A"]["paths"] == [["A", "B", "D"], ["A", "C", "D"]],
        str(suppressed["A"]),
    )

    # B recovers: A must stay suppressed via C, B becomes actionable if still down?
    # B is up now -> A's B-branch is cut, C-branch still all down.
    request("POST", "/reports", {"service": "B", "seq": 2, "status": "recover"})
    _, alerts = request("GET", "/alerts")
    actionable = [f["service"] for f in alerts["actionable_faults"]]
    suppressed = {f["service"]: f for f in alerts["suppressed_faults"]}
    check("after B recovery: D remains actionable", actionable == ["D"], str(alerts))
    check(
        "A remains suppressed via C only; B removed from suppressed (healthy)",
        "A" in suppressed and "B" not in suppressed
        and suppressed["A"]["paths"] == [["A", "C", "D"]],
        str(alerts),
    )

    # D recovers: C is still faulty with no faulty upstream, so C becomes the
    # root (actionable) and A stays suppressed via the all-down A->C path --
    # reclassification happens immediately, no resend needed.
    request("POST", "/reports", {"service": "D", "seq": 2, "status": "recover"})
    _, alerts = request("GET", "/alerts")
    actionable = sorted(f["service"] for f in alerts["actionable_faults"])
    suppressed = {f["service"]: f for f in alerts["suppressed_faults"]}
    check(
        "D recovery immediately makes C the new actionable root",
        actionable == ["C"],
        str(alerts),
    )
    check(
        "A reclassified to suppressed-by-C without resend",
        "A" in suppressed and suppressed["A"]["root_causes"] == ["C"]
        and suppressed["A"]["paths"] == [["A", "C"]],
        str(alerts),
    )

    # C recovers too: A is still faulty but no all-down path remains at all,
    # so it must immediately become actionable.
    request("POST", "/reports", {"service": "C", "seq": 2, "status": "recover"})
    _, alerts = request("GET", "/alerts")
    actionable = sorted(f["service"] for f in alerts["actionable_faults"])
    check(
        "once all all-down paths are cut, A becomes actionable without resend",
        actionable == ["A"],
        str(alerts),
    )
    check("no suppressed faults remain", alerts["suppressed_faults"] == [], str(alerts))
    check("healthy list includes recovered B, C and D",
          {"B", "C", "D"}.issubset(set(alerts["healthy_services"])), str(alerts))

    # Never-reported nodes cut paths.  Fresh graph: X down, Y never reported.
    reset()
    request("POST", "/graph", GRAPH_V1)
    fault("X", 1)
    _, alerts = request("GET", "/alerts")
    actionable = [f["service"] for f in alerts["actionable_faults"]]
    check("unreported upstream Y means X stays actionable", actionable == ["X"], str(alerts))
    check("Y listed as unreported", "Y" in alerts["unreported_services"], str(alerts))

    # A single isolated fault with no upstream is always actionable.
    fault("Y", 1)
    _, alerts = request("GET", "/alerts")
    actionable = sorted(f["service"] for f in alerts["actionable_faults"])
    suppressed = {f["service"] for f in alerts["suppressed_faults"]}
    check("Y root actionable", "Y" in actionable, str(alerts))
    check("X now suppressed by Y", "X" in suppressed, str(alerts))


def test_graph_replace_preserves_state() -> None:
    print("[T4] graph replacement preserves same-named states; dropped node state hidden")
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    request("POST", "/reports", {"service": "B", "seq": 7, "status": "fault"})
    request("POST", "/reports", {"service": "Y", "seq": 1, "status": "fault"})

    status, body = request("POST", "/graph", GRAPH_V2)
    check("v2 accepted", status == 200 and body["revision"] == 2, str(body))
    check(
        "same-named service states in the new graph are preserved",
        set(body["preserved_state"]) == {"B", "D"},
        str(body),
    )

    _, alerts = request("GET", "/alerts")
    check("D still down after swap", "D" in alerts["down_services"], str(alerts))
    check("B still down after swap", "B" in alerts["down_services"], str(alerts))
    check("dropped-node state not exposed", "Y" not in alerts["down_services"], str(alerts))
    check("new node Z reported as unreported", "Z" in alerts["unreported_services"], str(alerts))

    # Sequence numbers preserved as well: seq 7 already used for B.
    status, body = request("POST", "/reports", {"service": "B", "seq": 7, "status": "fault"})
    check("seq history preserved across swap (idempotent)", body["duplicate"] is True, str(body))
    status, _ = request("POST", "/reports", {"service": "B", "seq": 6, "status": "recover"})
    check("stale seq after swap still rejected", status == 409)

    # And if Y comes back in a later version, its old state resurfaces.
    status, _ = request("POST", "/graph", GRAPH_V1)
    _, alerts = request("GET", "/alerts")
    check("re-added Y shows its preserved down state", "Y" in alerts["down_services"], str(alerts))


def test_concurrency() -> None:
    print("[T5] concurrent interleavings")
    reset()
    request("POST", "/graph", GRAPH_V1)

    # 5a: parallel reports with distinct seqs for one service.  Under real
    # at processing time must be rejected (409) and state may never roll back.
    seqs = [2, 5, 3, 8, 1]
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = [
            pool.submit(request, "POST", "/reports",
                        {"service": "A", "seq": s, "status": "fault" if s % 2 else "recover"})
            for s in seqs
        ]
        results = [f.result() for f in futs]
    ok = [b for st, b in results if st == 200]
    stale_pairs = [(st, b) for st, b in results if st == 409 and "stale" in b["detail"]]
    stale_seqs = [s for s in seqs if any(f"seq {s} <" in b["detail"] for _, b in stale_pairs)]
    applied_seqs = sorted(b["seq"] for b in ok if b["applied"])
    check(
        "applied seqs form a strictly increasing chain",
        all(a < b for a, b in zip(applied_seqs, applied_seqs[1:])),
        str(results),
    )
    check(
        "a report rejected as stale is one that arrived out of order",
        all(s < max(applied_seqs) for s in stale_seqs),
        str(results),
    )
    check("no unexpected error codes", all(st in (200, 409) for st, _ in results), str(results))
    _, alerts = request("GET", "/alerts")
    # Final state matches the content of the highest-seq report (seq 8 -> recover),
    # proving a late low seq never rolled the state backwards.
    check(
        "state never rolled back: A reflects highest seq (seq 8 recover -> up)",
        "A" in alerts["healthy_services"],
        str(alerts),
    )

    # 5b: burst of identical (service, seq, content): one apply, rest idempotent.
    reset()
    request("POST", "/graph", GRAPH_V1)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(request, "POST", "/reports",
                            {"service": "A", "seq": 1, "status": "fault"})
                for _ in range(8)]
        burst = [f.result() for f in futs]
    applied = sum(1 for st, b in burst if st == 200 and b["applied"])
    dupes = sum(1 for st, b in burst if st == 200 and b["duplicate"])
    check("identical concurrent retries: exactly 1 apply", applied == 1, str(burst))
    check("identical concurrent retries: 7 idempotent", dupes == 7, str(burst))

    # 5c: same seq, different content in parallel: one wins, others 409, no crash.
    reset()
    request("POST", "/graph", GRAPH_V1)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = [
            pool.submit(request, "POST", "/reports",
                        {"service": "A", "seq": 1, "status": "fault"}),
            pool.submit(request, "POST", "/reports",
                        {"service": "A", "seq": 1, "status": "recover"}),
        ]
        race = [f.result() for f in futs]
    ok = [b for st, b in race if st == 200]
    conf = [st for st, _ in race if st == 409]
    check("same-seq conflicting race: one accepted, one 409",
          len(ok) == 1 and len(conf) == 1, str(race))

    # 5d: parallel graph replacements: every observed revision is a fully
    # committed graph; no mixed node/edge sets.
    alt_graphs = [
        {"version": f"g{i}", "services": [
            {"name": f"S{j}", "dependencies": [f"S{j-1}"] if j else []}
            for j in range(i + 1)
        ]}
        for i in range(1, 4)
    ]
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(request, "POST", "/graph", g) for g in alt_graphs for _ in range(2)]
        replacements = [f.result() for f in futs]
    check("all parallel replacements accepted", all(st == 200 for st, _ in replacements),
          str(replacements))
    status, graph = request("GET", "/graph")
    names = sorted(s["name"] for s in graph["services"])
    expected = sorted(
        s["name"]
        for s in next(g for g in alt_graphs if g["version"] == graph["version"])["services"]
    )
    check("final graph internally consistent", names == expected, str(graph))

    # 5e: replace + query interleaving: every alerts snapshot must be
    # internally consistent with exactly one committed revision.
    with ThreadPoolExecutor(max_workers=6) as pool:
        writer = [pool.submit(request, "POST", "/graph", g) for g in alt_graphs for _ in range(3)]
        reader = [pool.submit(request, "GET", "/alerts") for _ in range(30)]
        for f in writer:
            f.result()
        reads = [f.result() for f in reader]

    consistent = True
    for st, body in reads:
        if st != 200:
            consistent = False
            break
        nodes = set(body["services"])
        classified = (
            set(body["down_services"])
            | set(body["unreported_services"])
            | set(body["healthy_services"])
        )
        action = {f["service"] for f in body["actionable_faults"]}
        supp = {f["service"] for f in body["suppressed_faults"]}
        if classified != nodes:
            consistent = False  # every node belongs to exactly one class
        if action | supp != set(body["down_services"]) or action & supp:
            consistent = False  # every down node is either actionable or suppressed
    check(
        "every concurrent read is a consistent single-revision snapshot",
        consistent,
        str(reads[:2]),
    )


def test_phase_acknowledgement_basics() -> None:
    print("[T6] alert phases and acknowledgement basics")
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})

    status, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("actionable fault exposes phase 1", d["phase"] == 1, str(d))
    check("new phase starts unacknowledged", d["acknowledged"] is False
          and d["acknowledged_at"] is None, str(d))

    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 1})
    check("acknowledge current phase accepted",
          status == 200 and body["acknowledged"] is True
          and body["duplicate"] is False and body["phase"] == 1, str(body))
    first_ack_at = body.get("acknowledged_at")
    check("acknowledgement returns a timestamp",
          isinstance(first_ack_at, str) and first_ack_at, str(body))

    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 1})
    check("re-acknowledging current phase is idempotent",
          status == 200 and body["duplicate"] is True
          and body["acknowledged_at"] == first_ack_at, str(body))

    _, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("alerts reflect the acknowledgement",
          d["acknowledged"] is True and d["acknowledged_at"] == first_ack_at, str(d))

    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 2})
    check("future/unknown phase rejected 409 stale_phase",
          status == 409 and body["error"] == "stale_phase", str(body))

    status, body = request("POST", "/acknowledgements", {"service": "NOPE", "phase": 1})
    check("acknowledging unknown service -> 404",
          status == 404 and body["error"] == "unknown_service", str(body))

    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 0})
    check("phase <= 0 -> 422", status == 422, str(body))

    status, body = request("POST", "/acknowledgements", {"service": "B", "phase": 1})
    check("healthy/unreported service has no actionable phase -> 409",
          status == 409 and body["error"] == "stale_phase", str(body))

    # A suppressed fault is not actionable, so it cannot be acknowledged either.
    request("POST", "/reports", {"service": "B", "seq": 1, "status": "fault"})
    _, alerts = request("GET", "/alerts")
    check("B is now suppressed by D",
          any(f["service"] == "B" for f in alerts["suppressed_faults"]), str(alerts))
    status, body = request("POST", "/acknowledgements", {"service": "B", "phase": 1})
    check("suppressed fault cannot be acknowledged -> 409",
          status == 409 and body["error"] == "stale_phase", str(body))

    # Retries that do not change classification must not clear the ack.
    status, body = request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    check("same-seq retry stays idempotent", body["duplicate"] is True, str(body))
    _, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("idempotent retry keeps phase and acknowledgement",
          d["phase"] == 1 and d["acknowledged"] is True, str(d))

    status, body = request("POST", "/reports", {"service": "D", "seq": 2, "status": "fault"})
    check("repeated fault with higher seq applies",
          body["applied"] is True and body["current_status"] == "down", str(body))
    _, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("still-actionable service keeps its phase after an applied fault report",
          d["phase"] == 1 and d["acknowledged"] is True, str(d))


def test_phase_transitions() -> None:
    print("[T7] phase transitions: recovery, suppression, graph swaps")
    reset()
    request("POST", "/graph", GRAPH_V1)

    # Recovery then a new fault starts a fresh phase.
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    request("POST", "/acknowledgements", {"service": "D", "phase": 1})
    request("POST", "/reports", {"service": "D", "seq": 2, "status": "recover"})
    request("POST", "/reports", {"service": "D", "seq": 3, "status": "fault"})
    _, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("fault after recovery gets a new phase", d["phase"] == 2, str(d))
    check("new phase is unacknowledged", d["acknowledged"] is False, str(d))
    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 1})
    check("old phase ack rejected after recovery/re-fault",
          status == 409 and body["error"] == "stale_phase", str(body))
    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 2})
    check("new phase ack accepted", status == 200 and body["duplicate"] is False, str(body))

    # Suppression by a new upstream fault ends the phase; when the upstream
    # recovers the still-faulty downstream must get a new phase.
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "A", "seq": 1, "status": "fault"})
    request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    request("POST", "/reports", {"service": "B", "seq": 1, "status": "fault"})
    _, alerts = request("GET", "/alerts")
    check("A becomes suppressed by upstream B",
          any(f["service"] == "A" for f in alerts["suppressed_faults"]), str(alerts))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    check("acknowledgement cannot attach to a suppressed alert",
          status == 409 and body["error"] == "stale_phase", str(body))
    status, body = request("POST", "/acknowledgements", {"service": "B", "phase": 1})
    check("upstream root B is acknowledgeable", status == 200, str(body))

    request("POST", "/reports", {"service": "B", "seq": 2, "status": "recover"})
    _, alerts = request("GET", "/alerts")
    a = next(f for f in alerts["actionable_faults"] if f["service"] == "A")
    check("still-faulty A re-enters actionable with a new phase", a["phase"] == 2, str(a))
    check("the new phase is unacknowledged", a["acknowledged"] is False, str(a))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    check("pre-suppression acknowledgement is stale",
          status == 409 and body["error"] == "stale_phase", str(body))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 2})
    check("re-entered phase is acknowledgeable", status == 200, str(body))

    # Graph swaps that leave classification unchanged preserve phase and ack.
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    request("POST", "/reports", {"service": "B", "seq": 1, "status": "fault"})
    request("POST", "/acknowledgements", {"service": "D", "phase": 1})
    status, body = request("POST", "/graph", GRAPH_V2)
    check("same-classification swap accepted", status == 200, str(body))
    _, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("classification-unchanged swap keeps phase 1", d["phase"] == 1, str(d))
    check("classification-unchanged swap keeps the acknowledgement",
          d["acknowledged"] is True, str(d))
    status, body = request("POST", "/acknowledgements", {"service": "D", "phase": 1})
    check("ack still valid after classification-unchanged swap",
          status == 200, str(body))

    # A graph swap that removes the suppressing upstream makes the downstream
    # actionable again with a fresh phase; swapping back suppresses it again.
    # A had phase 1 while it was momentarily actionable (before B reported),
    # so the post-swap episodes are phases 2 and 3.
    reset()
    request("POST", "/graph", {
        "version": "chain",
        "services": [
            {"name": "A", "dependencies": ["B"]},
            {"name": "B", "dependencies": []},
        ],
    })
    request("POST", "/reports", {"service": "A", "seq": 1, "status": "fault"})
    request("POST", "/reports", {"service": "B", "seq": 1, "status": "fault"})
    _, alerts = request("GET", "/alerts")
    check("A suppressed by B before the swap",
          any(f["service"] == "A" for f in alerts["suppressed_faults"]), str(alerts))
    status, _ = request("POST", "/graph", {
        "version": "flat",
        "services": [
            {"name": "A", "dependencies": []},
            {"name": "B", "dependencies": []},
        ],
    })
    check("edge-removing swap accepted", status == 200)
    _, alerts = request("GET", "/alerts")
    a = next(f for f in alerts["actionable_faults"] if f["service"] == "A")
    check("swap-cut suppression gives A a fresh phase", a["phase"] == 2, str(a))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 2})
    check("freshly actionable A is acknowledgeable after swap", status == 200, str(body))
    request("POST", "/graph", {
        "version": "chain2",
        "services": [
            {"name": "A", "dependencies": ["B"]},
            {"name": "B", "dependencies": []},
        ],
    })
    _, alerts = request("GET", "/alerts")
    check("A is suppressed again after the edge returns",
          any(f["service"] == "A" for f in alerts["suppressed_faults"]), str(alerts))
    request("POST", "/graph", {
        "version": "flat2",
        "services": [
            {"name": "A", "dependencies": []},
            {"name": "B", "dependencies": []},
        ],
    })
    _, alerts = request("GET", "/alerts")
    a = next(f for f in alerts["actionable_faults"] if f["service"] == "A")
    check("A gets yet another phase after swap-cycle", a["phase"] == 3, str(a))
    check("the newest phase is unacknowledged", a["acknowledged"] is False, str(a))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 2})
    check("ack from the previous swap episode is stale",
          status == 409 and body["error"] == "stale_phase", str(body))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 3})
    check("ack for the newest swap episode is accepted", status == 200, str(body))

    # Removing the service from the graph and re-adding it also starts a new
    # phase if it is still faulty and actionable when it comes back.
    reset()
    request("POST", "/graph", {
        "services": [{"name": "A", "dependencies": []}],
    })
    request("POST", "/reports", {"service": "A", "seq": 1, "status": "fault"})
    request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    request("POST", "/graph", {"services": [{"name": "Z", "dependencies": []}]})
    _, alerts = request("GET", "/alerts")
    check("removed service disappears from alerts", alerts["services"] == ["Z"], str(alerts))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    check("removed service cannot be acknowledged -> 404",
          status == 404 and body["error"] == "unknown_service", str(body))
    request("POST", "/graph", {"services": [{"name": "A", "dependencies": []}]})
    _, alerts = request("GET", "/alerts")
    a = next(f for f in alerts["actionable_faults"] if f["service"] == "A")
    check("re-added still-faulty A gets a new phase", a["phase"] == 2, str(a))
    check("re-added A is unacknowledged", a["acknowledged"] is False, str(a))


def test_phase_concurrency() -> None:
    print("[T8] concurrent acknowledgement/report/graph interleavings")
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})

    # 8a: identical concurrent acknowledgements: exactly one first ack.
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [
            pool.submit(request, "POST", "/acknowledgements",
                        {"service": "D", "phase": 1})
            for _ in range(8)
        ]
        acks = [f.result() for f in futs]
    ok = [b for st, b in acks if st == 200]
    firsts = [b for b in ok if not b["duplicate"]]
    check("concurrent identical acks: all accepted", len(ok) == 8, str(acks))
    check("concurrent identical acks: exactly one non-duplicate",
          len(firsts) == 1, str(acks))
    check("concurrent identical acks share one timestamp",
          len({b["acknowledged_at"] for b in ok}) == 1, str(acks))

    # 8b: acknowledgement racing recovery.  It either commits before the
    # recovery (200, possibly duplicate) or is stale afterwards (409) -- never
    # a 500, and the final state has no actionable fault.
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})
    with ThreadPoolExecutor(max_workers=5) as pool:
        recover = pool.submit(request, "POST", "/reports",
                              {"service": "D", "seq": 2, "status": "recover"})
        acks_f = [pool.submit(request, "POST", "/acknowledgements",
                              {"service": "D", "phase": 1}) for _ in range(4)]
        recover_result = recover.result()
        acks = [f.result() for f in acks_f]
    check("racing recovery still applies", recover_result[0] == 200, str(recover_result))
    check("racing acks return 200 or 409 stale_phase only",
          all(st in (200, 409) for st, _ in acks)
          and all(st == 200 or b["error"] == "stale_phase" for st, b in acks),
          str(acks))
    _, alerts = request("GET", "/alerts")
    check("recovered D is not actionable", alerts["actionable_faults"] == [], str(alerts))
    # Regardless of which acks won the race, a later fault is a new phase that
    # must not inherit any of them.
    request("POST", "/reports", {"service": "D", "seq": 3, "status": "fault"})
    _, alerts = request("GET", "/alerts")
    d = next(f for f in alerts["actionable_faults"] if f["service"] == "D")
    check("re-fault after ack/recovery race is a new unacked phase",
          d["phase"] == 2 and d["acknowledged"] is False, str(d))

    # 8c: acknowledgement racing a report that suppresses the service.
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "A", "seq": 1, "status": "fault"})
    request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    with ThreadPoolExecutor(max_workers=5) as pool:
        fault_b = pool.submit(request, "POST", "/reports",
                              {"service": "B", "seq": 1, "status": "fault"})
        acks_f = [pool.submit(request, "POST", "/acknowledgements",
                              {"service": "A", "phase": 1}) for _ in range(4)]
        fault_b.result()
        acks = [f.result() for f in acks_f]
    check("ack racing a suppressing report: no unexpected status",
          all(st in (200, 409) for st, _ in acks), str(acks))
    _, alerts = request("GET", "/alerts")
    check("A stays suppressed after the race",
          any(f["service"] == "A" for f in alerts["suppressed_faults"]), str(alerts))
    status, body = request("POST", "/acknowledgements", {"service": "A", "phase": 1})
    check("post-race ack for suppressed A is stale",
          status == 409 and body["error"] == "stale_phase", str(body))
    # Upstream recovery re-opens A as a fresh, unacknowledged phase even though
    # some acks won the earlier race.
    request("POST", "/reports", {"service": "B", "seq": 2, "status": "recover"})
    _, alerts = request("GET", "/alerts")
    a = next(f for f in alerts["actionable_faults"] if f["service"] == "A")
    check("after upstream recovery A has a new unacked phase",
          a["phase"] == 2 and a["acknowledged"] is False, str(a))

    # 8d: reports, acknowledgements and graph swaps all interleaved.  Every
    # /alerts snapshot must be internally consistent and every ack must be
    # either accepted for the phase it names or rejected as stale.
    reset()
    request("POST", "/graph", GRAPH_V1)
    request("POST", "/reports", {"service": "D", "seq": 1, "status": "fault"})

    def flip(i: int):
        return request("POST", "/reports", {
            "service": "D",
            "seq": 100 + i,
            "status": "fault" if i % 3 else "recover",
        })

    def swap(i: int):
        return request("POST", "/graph", {**GRAPH_V2, "version": f"gen{i}"})

    def read_and_ack(i: int):
        status, body = request("GET", "/alerts")
        if status != 200:
            return body, None
        phases = {f["service"]: f["phase"] for f in body["actionable_faults"]}
        if "D" not in phases:
            return body, None
        ast, abody = request("POST", "/acknowledgements",
                             {"service": "D", "phase": phases["D"]})
        return body, (ast, abody, phases["D"])

    # Guarantee at least one interleaved acknowledgement is exercised even if
    # the first writer flips D to recover before any reader runs.
    _, baseline = request("GET", "/alerts")
    base_phase = next(
        (f["phase"] for f in baseline["actionable_faults"] if f["service"] == "D"),
        None,
    )
    if base_phase is not None:
        ast, abody = request("POST", "/acknowledgements",
                             {"service": "D", "phase": base_phase})
        check("baseline acknowledgement before the interleaving succeeds",
              ast == 200, str(abody))

    with ThreadPoolExecutor(max_workers=10) as pool:
        reader_futs = [pool.submit(read_and_ack, i) for i in range(12)]
        writer_futs = []
        for i in range(12):
            writer_futs.append(pool.submit(flip, i))
            writer_futs.append(pool.submit(swap, i))
        reads = [f.result() for f in reader_futs]
        writes = [f.result() for f in writer_futs]

    check("interleaved graph swaps all succeed",
          all(st == 200 for st, _ in writes[1::2]), str(writes))
    check("interleaved reports are applied or rejected as stale/conflicting only",
          all(st in (200, 409) for st, _ in writes[0::2])
          and all(st == 200 or b["error"] == "conflict" for st, b in writes[0::2]),
          str(writes))

    snapshots = [body for body, _ in reads]
    consistent = True
    for body in snapshots:
        action = {f["service"] for f in body["actionable_faults"]}
        supp = {f["service"] for f in body["suppressed_faults"]}
        if action & supp or action | supp != set(body["down_services"]):
            consistent = False
        for fault in body["actionable_faults"]:
            if not isinstance(fault.get("phase"), int) or fault["phase"] < 1:
                consistent = False
            if fault["acknowledged"] != (fault["acknowledged_at"] is not None):
                consistent = False
    check("every interleaved /alerts snapshot is consistent", consistent, str(snapshots[:2]))

    # The reader may legitimately observe D as not-actionable (a concurrent
    # flip recovered it), or observe a phase that another writer supersedes
    # before the ack lands.  Either way, the only allowed outcomes are an ACK
    # for that exact phase or a 409 stale_phase -- never an error or a silent
    # acknowledgement of a different phase.
    ack_attempts = [ack for _, ack in reads if ack is not None]
    check("interleaved ack attempts return 200 or 409 stale_phase only",
          all(st in (200, 409) for st, _, _ in ack_attempts)
          and all(st == 200 or b["error"] == "stale_phase"
                  for st, b, _ in ack_attempts),
          str(ack_attempts))
    check("at least one interleaved ack was exercised", ack_attempts != [], str(reads))

    # Do one more read/ack after the storm so the path is exercised regardless
    # of scheduling: with no concurrent writer, reading and then acknowledging
    # the phase must succeed and be visible on that exact phase.
    post_body, post_ack = read_and_ack(0)
    if post_ack is not None:
        pst, pb, pphase = post_ack
        check("post-interleaving read-then-ack succeeds", pst == 200, str(pb))
        _, after = request("GET", "/alerts")
        cur = next(
            (f for f in after["actionable_faults"] if f["service"] == "D"), None
        )
        check("post-interleaving ack is visible on the current phase",
              cur is not None and cur["phase"] == pphase
              and cur["acknowledged"] is True
              and cur["acknowledged_at"] == pb["acknowledged_at"], str(cur))

    # Every ack that succeeded must echo exactly the phase it was issued for.
    check("successful acks echo the phase they acknowledged",
          all(b["phase"] == phase for st, b, phase in ack_attempts if st == 200),
          str(ack_attempts))

    # Final invariant: every currently acknowledged phase was named by a
    # successful ack for exactly that service and phase number -- no ack can
    # ever be attributed to a phase it did not name.
    _, alerts = request("GET", "/alerts")
    all_acks = list(ack_attempts)
    if post_ack is not None:
        all_acks.append(post_ack)
    for fault in alerts["actionable_faults"]:
        if fault["acknowledged"]:
            check(
                f"acknowledged phase matches a successful ack for {fault['service']}",
                any(st == 200 and body["service"] == fault["service"]
                    and body["phase"] == fault["phase"]
                    and body["acknowledged_at"] == fault["acknowledged_at"]
                    for st, body, _ in all_acks),
                str(fault) + " " + str(all_acks),
            )


def main() -> int:
    print(f"Self-test against {BASE_URL}\n")
    try:
        urllib.request.urlopen(BASE_URL + "/health", timeout=5)
    except Exception as exc:  # noqa: BLE001
        print(f"server not reachable: {exc}")
        return 2

    test_graph_validation()
    test_report_semantics()
    test_suppression()
    test_graph_replace_preserves_state()
    test_concurrency()
    test_phase_acknowledgement_basics()
    test_phase_transitions()
    test_phase_concurrency()

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
