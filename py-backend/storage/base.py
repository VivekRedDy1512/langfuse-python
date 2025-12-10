from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class TraceQuery:
    project_id: str
    limit: int = 50
    offset: int = 0
    user_id: Optional[str] = None
    name: Optional[str] = None
    start_from: Optional[datetime] = None
    start_to: Optional[datetime] = None


class TraceStore(ABC):
    @abstractmethod
    def insert_batch(self, events: List[Dict[str, Any]]) -> None:
        ...

    @abstractmethod
    def get_traces(self, query: TraceQuery) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def get_trace_by_id(
        self, project_id: str, trace_id: str
    ) -> Optional[Dict[str, Any]]:
        ...

# ---------- SPANS ----------

@dataclass
class SpanQuery:
    project_id: str
    trace_id: Optional[str] = None
    limit: int = 100
    offset: int = 0


class SpanStore(ABC):
    @abstractmethod
    def insert_batch(self, spans: List[Dict[str, Any]]) -> None:
        ...

    @abstractmethod
    def get_spans(self, query: SpanQuery) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def get_span_by_id(
        self, project_id: str, span_id: str
    ) -> Optional[Dict[str, Any]]:
        ...


# ---------- EVALS ----------

@dataclass
class EvalQuery:
    project_id: str
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    limit: int = 100
    offset: int = 0


class EvalStore(ABC):
    @abstractmethod
    def insert_batch(self, evals: List[Dict[str, Any]]) -> None:
        ...

    @abstractmethod
    def get_evals(self, query: EvalQuery) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def get_eval_by_id(
        self, project_id: str, eval_id: str
    ) -> Optional[Dict[str, Any]]:
        ...