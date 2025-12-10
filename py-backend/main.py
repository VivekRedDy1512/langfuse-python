from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from storage.base import TraceStore, TraceQuery, SpanStore, SpanQuery, EvalStore, EvalQuery
from storage.bigquery_store import BigQueryTraceStore
from storage.bigquery_span_store import BigQuerySpanStore
from storage.bigquery_eval_store import BigQueryEvalStore
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Python LLM Observability Backend")


def get_trace_store() -> TraceStore:
    backend = os.getenv("TRACE_BACKEND", "bigquery")
    if backend == "bigquery":
        project_id = os.getenv("GCP_PROJECT_ID")
        dataset = os.getenv("BQ_DATASET", "llm_observability")
        table = os.getenv("BQ_TABLE", "traces")
        if not project_id:
            raise RuntimeError("GCP_PROJECT_ID is not set")
        return BigQueryTraceStore(project_id=project_id, dataset=dataset, table=table)
    raise RuntimeError(f"Unsupported TRACE_BACKEND={backend}")

def get_span_store() -> SpanStore:
    backend = os.getenv("SPAN_BACKEND", "bigquery")
    if backend == "bigquery":
        project_id = os.getenv("GCP_PROJECT_ID")
        dataset = os.getenv("BQ_DATASET", "llm_observability")
        if not project_id:
            raise RuntimeError("GCP_PROJECT_ID is not set")
        return BigQuerySpanStore(project_id=project_id, dataset=dataset)
    raise RuntimeError(f"Unsupported SPAN_BACKEND={backend}")


def get_eval_store() -> EvalStore:
    backend = os.getenv("EVAL_BACKEND", "bigquery")
    if backend == "bigquery":
        project_id = os.getenv("GCP_PROJECT_ID")
        dataset = os.getenv("BQ_DATASET", "llm_observability")
        if not project_id:
            raise RuntimeError("GCP_PROJECT_ID is not set")
        return BigQueryEvalStore(project_id=project_id, dataset=dataset)
    raise RuntimeError(f"Unsupported EVAL_BACKEND={backend}")



class IngestEvent(BaseModel):
    project_id: str
    trace_id: str
    timestamp: datetime
    end_time: Optional[datetime] = None
    name: Optional[str] = None
    user_id: Optional[str] = None
    status: Optional[str] = None
    latency_ms: Optional[int] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TraceResponse(BaseModel):
    trace_id: str
    project_id: str
    timestamp: datetime
    end_time: Optional[datetime] = None
    name: Optional[str] = None
    user_id: Optional[str] = None
    status: Optional[str] = None
    latency_ms: Optional[int] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ---------- SPANS ----------

class SpanIngestEvent(BaseModel):
    project_id: str
    trace_id: str
    span_id: str
    timestamp: datetime
    end_time: Optional[datetime] = None
    name: Optional[str] = None
    type: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SpanResponse(BaseModel):
    project_id: str
    trace_id: str
    span_id: str
    timestamp: Optional[datetime]
    end_time: Optional[datetime] = None
    name: Optional[str] = None
    type: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ---------- EVALS ----------

class EvalIngestEvent(BaseModel):
    project_id: str
    eval_id: str
    score_name: str
    score_value: Optional[float] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    evaluator: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class EvalResponse(BaseModel):
    project_id: str
    eval_id: str
    score_name: str
    score_value: Optional[float] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    evaluator: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None



@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest")
def ingest_events(
    events: List[IngestEvent],
    store: TraceStore = Depends(get_trace_store),
) -> Dict[str, int]:
    payloads = [e.model_dump() for e in events]
    store.insert_batch(payloads)
    return {"inserted": len(events)}


@app.get("/traces", response_model=List[TraceResponse])
def list_traces(
    project_id: str,
    limit: int = 50,
    offset: int = 0,
    store: TraceStore = Depends(get_trace_store),
) -> List[TraceResponse]:
    q = TraceQuery(project_id=project_id, limit=limit, offset=offset)
    rows = store.get_traces(q)
    return [TraceResponse(**row) for row in rows]


@app.get("/traces/{trace_id}", response_model=TraceResponse)
def get_trace(
    trace_id: str,
    project_id: str,
    store: TraceStore = Depends(get_trace_store),
) -> TraceResponse:
    row = store.get_trace_by_id(project_id=project_id, trace_id=trace_id)
    if not row:
        raise HTTPException(status_code=404, detail="Trace not found")
    return TraceResponse(**row)


@app.post("/spans/ingest")
def ingest_spans(
    spans: List[SpanIngestEvent],
    store: SpanStore = Depends(get_span_store),
) -> Dict[str, int]:
    payloads = [s.model_dump() for s in spans]
    store.insert_batch(payloads)
    return {"inserted": len(spans)}


@app.get("/spans", response_model=List[SpanResponse])
def list_spans(
    project_id: str,
    trace_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    store: SpanStore = Depends(get_span_store),
) -> List[SpanResponse]:
    q = SpanQuery(project_id=project_id, trace_id=trace_id, limit=limit, offset=offset)
    rows = store.get_spans(q)
    return [SpanResponse(**row) for row in rows]


@app.get("/spans/{span_id}", response_model=SpanResponse)
def get_span(
    span_id: str,
    project_id: str,
    store: SpanStore = Depends(get_span_store),
) -> SpanResponse:
    row = store.get_span_by_id(project_id=project_id, span_id=span_id)
    if not row:
        raise HTTPException(status_code=404, detail="Span not found")
    return SpanResponse(**row)



@app.post("/evals/ingest")
def ingest_evals(
    evals: List[EvalIngestEvent],
    store: EvalStore = Depends(get_eval_store),
) -> Dict[str, int]:
    payloads = [e.model_dump() for e in evals]
    # default created_at if missing
    now = datetime.utcnow()
    for p in payloads:
        if p.get("created_at") is None:
            p["created_at"] = now
    store.insert_batch(payloads)
    return {"inserted": len(evals)}


@app.get("/evals", response_model=List[EvalResponse])
def list_evals(
    project_id: str,
    trace_id: Optional[str] = None,
    span_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    store: EvalStore = Depends(get_eval_store),
) -> List[EvalResponse]:
    q = EvalQuery(
        project_id=project_id,
        trace_id=trace_id,
        span_id=span_id,
        limit=limit,
        offset=offset,
    )
    rows = store.get_evals(q)
    return [EvalResponse(**row) for row in rows]


@app.get("/evals/{eval_id}", response_model=EvalResponse)
def get_eval(
    eval_id: str,
    project_id: str,
    store: EvalStore = Depends(get_eval_store),
) -> EvalResponse:
    row = store.get_eval_by_id(project_id=project_id, eval_id=eval_id)
    if not row:
        raise HTTPException(status_code=404, detail="Eval not found")
    return EvalResponse(**row)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)