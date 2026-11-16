"""FastAPI application: dependency-alert API."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .database import close_connection, get_connection, init_db, write_tx
from .errors import (
    GraphError,
    NoGraph,
    ReportConflict,
    StalePhase,
    UnknownService,
)
from .graph_service import (
    backfill_phases,
    classify_alerts,
    get_graph,
    replace_graph,
)
from .phase_service import record_acknowledgement
from .report_service import submit_report
from .schemas import (
    AcknowledgementPayload,
    AcknowledgementResponse,
    AlertsResponse,
    GraphPayload,
    GraphResponse,
    ReplaceResponse,
    ReportPayload,
    ReportResponse,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    try:
        backfill_phases()
    finally:
        # The lifespan runs on its own thread; do not leave that thread's
        # request-scoped connection open for the process lifetime.
        close_connection()
    yield


app = FastAPI(
    title="Dependency Alert API",
    version="1.1.0",
    description=(
        "Service dependency graph + fault/recovery reports with monotonic "
        "sequence numbers, upstream-root-cause alert suppression and "
        "phase-scoped handling acknowledgements."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def _db_session(request: Request, call_next):
    # One SQLite connection per request (thread-local); always released.
    get_connection()
    try:
        return await call_next(request)
    finally:
        close_connection()


@app.exception_handler(GraphError)
async def _graph_error(_: Request, exc: GraphError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(ReportConflict)
async def _conflict(_: Request, exc: ReportConflict):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(UnknownService)
async def _unknown(_: Request, exc: UnknownService):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(NoGraph)
async def _no_graph(_: Request, exc: NoGraph):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.exception_handler(StalePhase)
async def _stale_phase(_: Request, exc: StalePhase):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": str(exc)},
    )


@app.post("/graph", response_model=ReplaceResponse, status_code=200)
def put_graph(payload: GraphPayload) -> ReplaceResponse:
    """Replace the **whole** dependency graph.

    Invalid payloads (cycles, unknown references, duplicate names/edges) are
    rejected and the previously committed graph is retained.  States of
    same-named services survive the replacement.
    """
    return ReplaceResponse(**replace_graph(payload))


@app.get("/graph", response_model=GraphResponse)
def read_graph() -> GraphResponse:
    """Return the currently committed graph."""
    return get_graph()


@app.post("/reports", response_model=ReportResponse, status_code=200)
def post_report(payload: ReportPayload) -> ReportResponse:
    """Report a service fault or recovery with a per-service increasing seq."""
    return ReportResponse(**submit_report(payload))


@app.get("/alerts", response_model=AlertsResponse)
def read_alerts() -> AlertsResponse:
    """Return actionable/suppressed faults, root causes and alert phases.

    Every actionable fault carries the ``phase`` to acknowledge.  A phase
    changes whenever the service newly becomes actionable (after recovery, or
    after an upstream suppression ends), so an old acknowledgement can never
    mask a newly actionable alert.
    """
    return AlertsResponse(**classify_alerts())


@app.post(
    "/acknowledgements",
    response_model=AcknowledgementResponse,
    status_code=200,
)
def post_acknowledgement(
    payload: AcknowledgementPayload,
) -> AcknowledgementResponse:
    """Record that the on-call engineer is handling a specific alert phase.

    The ``phase`` must equal the current actionable phase of ``service`` as
    returned by ``GET /alerts``.  Stale phases (the service recovered, a new
    episode started, or it is currently suppressed) are rejected with
    ``409 stale_phase`` and no acknowledgement is written.  Acknowledging the
    same current phase again is idempotent.
    """
    return AcknowledgementResponse(**record_acknowledgement(
        payload.service, payload.phase
    ))


@app.post("/admin/reset", status_code=200)
def admin_reset() -> dict:
    """Clear all data (test/operations helper)."""
    with write_tx() as conn:
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM alert_phase")
        conn.execute("DELETE FROM service_state")
        conn.execute("DELETE FROM edges")
        conn.execute("DELETE FROM nodes")
        conn.execute("DELETE FROM graph_meta")
    return {"reset": True}


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
