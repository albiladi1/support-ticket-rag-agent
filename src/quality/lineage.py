"""
Deliverable 5 (part of): OpenLineage events.

Emits START/COMPLETE (or FAIL) lineage events for each pipeline stage so
the full ingest -> bronze -> silver -> gold -> index flow is observable
and auditable, independent of Airflow's own UI.
"""
import os
import uuid
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.run import Run, RunEvent, RunState, Job
from openlineage.client.uuid import generate_new_uuid

NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "support-ticket-rag-agent")
_client = None


def get_client() -> OpenLineageClient:
    global _client
    if _client is None:
        _client = OpenLineageClient(url=os.getenv("OPENLINEAGE_URL", "http://localhost:5000"))
    return _client


def emit_event(job_name: str, state: RunState, run_id: str | None = None) -> str:
    """Emits a lineage event for `job_name` (e.g. 'bronze_ingest',
    'silver_merge', 'gold_merge', 'vector_index_build') and returns the
    run_id so START/COMPLETE pairs can be correlated."""
    client = get_client()
    run_id = run_id or str(generate_new_uuid())

    event = RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=job_name),
        producer="https://github.com/your-org/support-ticket-rag-agent",
    )
    client.emit(event)
    print(f"[openlineage] {job_name} -> {state.name} (run_id={run_id})")
    return run_id


class LineageStage:
    """Context manager: emits START on enter, COMPLETE on clean exit,
    FAIL if an exception propagates.

    Usage:
        with LineageStage("silver_merge"):
            upsert_silver(...)
    """

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.run_id = None

    def __enter__(self):
        self.run_id = emit_event(self.job_name, RunState.START)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        state = RunState.FAIL if exc_type else RunState.COMPLETE
        emit_event(self.job_name, state, run_id=self.run_id)
        return False  # do not swallow exceptions