from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from .base import EvalStore, EvalQuery


class BigQueryEvalStore(EvalStore):
    def __init__(self, project_id: str, dataset: str, table: str = "evals") -> None:
        self.client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.{dataset}.{table}"

    def insert_batch(self, evals: List[Dict[str, Any]]) -> None:
        if not evals:
            return

        for e in evals:
            created_at = e.get("created_at")
            created_at_val = created_at if isinstance(created_at, datetime) else created_at

            metadata_dict = e.get("metadata") or {}
            metadata_str = json.dumps(metadata_dict)

            sql = f"""
            INSERT `{self.table_id}` (
              eval_id,
              trace_id,
              span_id,
              project_id,
              score_name,
              score_value,
              evaluator,
              metadata,
              created_at
            )
            VALUES (
              @eval_id,
              @trace_id,
              @span_id,
              @project_id,
              @score_name,
              @score_value,
              @evaluator,
              @metadata,
              @created_at
            )
            """

            params = [
                bigquery.ScalarQueryParameter("eval_id", "STRING", e["eval_id"]),
                bigquery.ScalarQueryParameter("trace_id", "STRING", e.get("trace_id")),
                bigquery.ScalarQueryParameter("span_id", "STRING", e.get("span_id")),
                bigquery.ScalarQueryParameter("project_id", "STRING", e["project_id"]),
                bigquery.ScalarQueryParameter("score_name", "STRING", e["score_name"]),
                bigquery.ScalarQueryParameter(
                    "score_value", "FLOAT64", e.get("score_value")
                ),
                bigquery.ScalarQueryParameter(
                    "evaluator", "STRING", e.get("evaluator")
                ),
                bigquery.ScalarQueryParameter("metadata", "STRING", metadata_str),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", created_at_val),
            ]

            job_config = bigquery.QueryJobConfig(query_parameters=params)
            job = self.client.query(sql, job_config=job_config)
            job.result()

    def get_evals(self, query: EvalQuery) -> List[Dict[str, Any]]:
        clauses = ["project_id = @project_id"]
        params: List[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("project_id", "STRING", query.project_id)
        ]

        if query.trace_id:
            clauses.append("trace_id = @trace_id")
            params.append(
                bigquery.ScalarQueryParameter("trace_id", "STRING", query.trace_id)
            )
        if query.span_id:
            clauses.append("span_id = @span_id")
            params.append(
                bigquery.ScalarQueryParameter("span_id", "STRING", query.span_id)
            )

        where = " AND ".join(clauses)

        sql = f"""
        SELECT
          eval_id,
          trace_id,
          span_id,
          project_id,
          score_name,
          score_value,
          evaluator,
          metadata,
          created_at
        FROM `{self.table_id}`
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT @limit OFFSET @offset
        """

        params.append(bigquery.ScalarQueryParameter("limit", "INT64", query.limit))
        params.append(bigquery.ScalarQueryParameter("offset", "INT64", query.offset))

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.client.query(sql, job_config=job_config)

        out: List[Dict[str, Any]] = []
        for row in result:
            out.append(
                {
                    "eval_id": row.eval_id,
                    "trace_id": row.trace_id,
                    "span_id": row.span_id,
                    "project_id": row.project_id,
                    "score_name": row.score_name,
                    "score_value": row.score_value,
                    "evaluator": row.evaluator,
                    "metadata": json.loads(row.metadata) if row.metadata else {},
                    "created_at": row.created_at.isoformat()
                    if row.created_at
                    else None,
                }
            )
        return out

    def get_eval_by_id(
        self, project_id: str, eval_id: str
    ) -> Optional[Dict[str, Any]]:
        sql = f"""
        SELECT
          eval_id,
          trace_id,
          span_id,
          project_id,
          score_name,
          score_value,
          evaluator,
          metadata,
          created_at
        FROM `{self.table_id}`
        WHERE project_id = @project_id AND eval_id = @eval_id
        LIMIT 1
        """

        params = [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("eval_id", "STRING", eval_id),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        rows = list(self.client.query(sql, job_config=job_config))

        if not rows:
            return None

        row = rows[0]
        return {
            "eval_id": row.eval_id,
            "trace_id": row.trace_id,
            "span_id": row.span_id,
            "project_id": row.project_id,
            "score_name": row.score_name,
            "score_value": row.score_value,
            "evaluator": row.evaluator,
            "metadata": json.loads(row.metadata) if row.metadata else {},
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
