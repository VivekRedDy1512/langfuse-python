from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from .base import SpanStore, SpanQuery


class BigQuerySpanStore(SpanStore):
    def __init__(self, project_id: str, dataset: str, table: str = "spans") -> None:
        self.client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.{dataset}.{table}"

    def insert_batch(self, spans: List[Dict[str, Any]]) -> None:
        if not spans:
            return

        for s in spans:
            ts = s.get("timestamp") or s.get("start_time")
            end_ts = s.get("end_time")

            start_time_val = ts if isinstance(ts, datetime) else ts
            end_time_val = end_ts if isinstance(end_ts, datetime) else end_ts

            metadata_dict = s.get("metadata") or {}
            metadata_str = json.dumps(metadata_dict)

            sql = f"""
            INSERT `{self.table_id}` (
              span_id,
              trace_id,
              project_id,
              start_time,
              end_time,
              name,
              type,
              metadata
            )
            VALUES (
              @span_id,
              @trace_id,
              @project_id,
              @start_time,
              @end_time,
              @name,
              @type,
              @metadata
            )
            """

            params = [
                bigquery.ScalarQueryParameter("span_id", "STRING", s["span_id"]),
                bigquery.ScalarQueryParameter("trace_id", "STRING", s["trace_id"]),
                bigquery.ScalarQueryParameter("project_id", "STRING", s["project_id"]),
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time_val),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time_val),
                bigquery.ScalarQueryParameter("name", "STRING", s.get("name")),
                bigquery.ScalarQueryParameter("type", "STRING", s.get("type")),
                bigquery.ScalarQueryParameter("metadata", "STRING", metadata_str),
            ]

            job_config = bigquery.QueryJobConfig(query_parameters=params)
            job = self.client.query(sql, job_config=job_config)
            job.result()

    def get_spans(self, query: SpanQuery) -> List[Dict[str, Any]]:
        clauses = ["project_id = @project_id"]
        params: List[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("project_id", "STRING", query.project_id)
        ]

        if query.trace_id:
            clauses.append("trace_id = @trace_id")
            params.append(
                bigquery.ScalarQueryParameter("trace_id", "STRING", query.trace_id)
            )

        where = " AND ".join(clauses)

        sql = f"""
        SELECT
          span_id,
          trace_id,
          project_id,
          start_time,
          end_time,
          name,
          type,
          metadata
        FROM `{self.table_id}`
        WHERE {where}
        ORDER BY start_time ASC
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
                    "span_id": row.span_id,
                    "trace_id": row.trace_id,
                    "project_id": row.project_id,
                    "timestamp": row.start_time.isoformat() if row.start_time else None,
                    "end_time": row.end_time.isoformat() if row.end_time else None,
                    "name": row.name,
                    "type": row.type,
                    "metadata": json.loads(row.metadata) if row.metadata else {},
                }
            )
        return out

    def get_span_by_id(
        self, project_id: str, span_id: str
    ) -> Optional[Dict[str, Any]]:
        sql = f"""
        SELECT
          span_id,
          trace_id,
          project_id,
          start_time,
          end_time,
          name,
          type,
          metadata
        FROM `{self.table_id}`
        WHERE project_id = @project_id AND span_id = @span_id
        LIMIT 1
        """

        params = [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("span_id", "STRING", span_id),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        rows = list(self.client.query(sql, job_config=job_config))

        if not rows:
            return None

        row = rows[0]
        return {
            "span_id": row.span_id,
            "trace_id": row.trace_id,
            "project_id": row.project_id,
            "timestamp": row.start_time.isoformat() if row.start_time else None,
            "end_time": row.end_time.isoformat() if row.end_time else None,
            "name": row.name,
            "type": row.type,
            "metadata": json.loads(row.metadata) if row.metadata else {},
        }
