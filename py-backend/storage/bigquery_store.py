from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from .base import TraceStore, TraceQuery


class BigQueryTraceStore(TraceStore):
    def __init__(self, project_id: str, dataset: str, table: str) -> None:
        self.client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.{dataset}.{table}"

    def insert_batch(self, events: List[Dict[str, Any]]) -> None:
        """
        Insert events into BigQuery using standard SQL INSERT statements.
        This works in free tier and avoids streaming insert restrictions.
        """
        if not events:
            return

        for e in events:
            ts = e.get("timestamp")
            end_ts = e.get("end_time")

            # BigQuery TIMESTAMP parameters can be datetime or ISO strings.
            start_time_val = ts if isinstance(ts, datetime) else ts
            end_time_val = end_ts if isinstance(end_ts, datetime) else end_ts

            # Serialize metadata dict to JSON string to match STRING column
            metadata_dict = e.get("metadata") or {}
            metadata_str = json.dumps(metadata_dict)

            sql = f"""
            INSERT `{self.table_id}` (
              trace_id,
              project_id,
              start_time,
              end_time,
              name,
              user_id,
              status,
              latency_ms,
              input_tokens,
              output_tokens,
              metadata
            )
            VALUES (
              @trace_id,
              @project_id,
              @start_time,
              @end_time,
              @name,
              @user_id,
              @status,
              @latency_ms,
              @input_tokens,
              @output_tokens,
              @metadata
            )
            """

            params = [
                bigquery.ScalarQueryParameter("trace_id", "STRING", e["trace_id"]),
                bigquery.ScalarQueryParameter("project_id", "STRING", e["project_id"]),
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time_val),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time_val),
                bigquery.ScalarQueryParameter("name", "STRING", e.get("name")),
                bigquery.ScalarQueryParameter("user_id", "STRING", e.get("user_id")),
                bigquery.ScalarQueryParameter("status", "STRING", e.get("status")),
                bigquery.ScalarQueryParameter("latency_ms", "INT64", e.get("latency_ms")),
                bigquery.ScalarQueryParameter("input_tokens", "INT64", e.get("input_tokens")),
                bigquery.ScalarQueryParameter("output_tokens", "INT64", e.get("output_tokens")),
                bigquery.ScalarQueryParameter("metadata", "STRING", metadata_str),
            ]

            job_config = bigquery.QueryJobConfig(query_parameters=params)
            job = self.client.query(sql, job_config=job_config)
            job.result()  # force execution, raise if error

    def get_traces(self, query: TraceQuery) -> List[Dict[str, Any]]:
        clauses = ["project_id = @project_id"]
        params: List[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("project_id", "STRING", query.project_id)
        ]

        if query.user_id:
            clauses.append("user_id = @user_id")
            params.append(bigquery.ScalarQueryParameter("user_id", "STRING", query.user_id))

        if query.name:
            clauses.append("name = @name")
            params.append(bigquery.ScalarQueryParameter("name", "STRING", query.name))

        if query.start_from:
            clauses.append("start_time >= @start_from")
            params.append(
                bigquery.ScalarQueryParameter("start_from", "TIMESTAMP", query.start_from)
            )
        if query.start_to:
            clauses.append("start_time <= @start_to")
            params.append(
                bigquery.ScalarQueryParameter("start_to", "TIMESTAMP", query.start_to)
            )

        where = " AND ".join(clauses)

        sql = f"""
        SELECT
          trace_id,
          project_id,
          start_time,
          end_time,
          name,
          user_id,
          status,
          latency_ms,
          input_tokens,
          output_tokens,
          metadata
        FROM `{self.table_id}`
        WHERE {where}
        ORDER BY start_time DESC
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
                    "trace_id": row.trace_id,
                    "project_id": row.project_id,
                    "timestamp": row.start_time.isoformat() if row.start_time else None,
                    "end_time": row.end_time.isoformat() if row.end_time else None,
                    "name": row.name,
                    "user_id": row.user_id,
                    "status": row.status,
                    "latency_ms": row.latency_ms,
                    "input_tokens": row.input_tokens,
                    "output_tokens": row.output_tokens,
                    # metadata comes back as STRING; you can leave it as string or json.loads it
                    "metadata": json.loads(row.metadata) if row.metadata else {},
                }
            )

        return out

    def get_trace_by_id(
        self, project_id: str, trace_id: str
    ) -> Optional[Dict[str, Any]]:
        sql = f"""
        SELECT
          trace_id,
          project_id,
          start_time,
          end_time,
          name,
          user_id,
          status,
          latency_ms,
          input_tokens,
          output_tokens,
          metadata
        FROM `{self.table_id}`
        WHERE project_id = @project_id AND trace_id = @trace_id
        LIMIT 1
        """

        params = [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("trace_id", "STRING", trace_id),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        rows = list(self.client.query(sql, job_config=job_config))

        if not rows:
            return None

        row = rows[0]
        return {
            "trace_id": row.trace_id,
            "project_id": row.project_id,
            "timestamp": row.start_time.isoformat() if row.start_time else None,
            "end_time": row.end_time.isoformat() if row.end_time else None,
            "name": row.name,
            "user_id": row.user_id,
            "status": row.status,
            "latency_ms": row.latency_ms,
            "input_tokens": row.input_tokens,
            "output_tokens": row.output_tokens,
            "metadata": json.loads(row.metadata) if row.metadata else {},
        }
