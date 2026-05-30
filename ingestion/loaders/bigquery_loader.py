"""
BigQuery loader.

Loads a list of dicts into a BigQuery table using the google-cloud-bigquery client.
Uses schema autodetect for the raw layer (flexibility > strictness at this stage).
Strict schemas are enforced downstream by dbt contracts.

Env vars required:
    BIGQUERY_PROJECT   — GCP project ID
    BIGQUERY_DATASET   — target dataset name (e.g. "raw")
    GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON
"""

import logging
import os
from datetime import date

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Loads records into BigQuery raw dataset tables."""

    def __init__(self):
        self.project = os.environ["BIGQUERY_PROJECT"]
        self.dataset = os.environ.get("BIGQUERY_DATASET", "raw")
        self.client = bigquery.Client(project=self.project)

    def load(
        self,
        source_name: str,
        records: list[dict],
        partition_date: str | None = None,
    ) -> int:
        """
        Load records for a given source into BigQuery.

        Table naming convention: raw.{source_name}_raw
        Partitioned by load date using a _partition_date column.

        Returns:
            Number of rows inserted.
        """
        if not records:
            logger.info("[%s] No records to load — skipping.", source_name)
            return 0

        target_date = partition_date or str(date.today())
        table_id = f"{self.project}.{self.dataset}.{source_name}_raw"

        # Inject partition column so we can filter/debug by load date
        enriched = [
            {**record, "_partition_date": target_date, "_loaded_at": self._now()}
            for record in records
        ]

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,                 # raw layer: let BQ infer schema
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        job = self.client.load_table_from_json(
            enriched,
            table_id,
            job_config=job_config,
        )

        job.result()  # blocks until complete

        table = self.client.get_table(table_id)
        logger.info(
            "[%s] Loaded %d rows into %s (total rows in table: %d)",
            source_name, len(records), table_id, table.num_rows,
        )
        return len(records)

    def load_all(
        self,
        source_records: dict[str, list[dict]],
        partition_date: str | None = None,
    ) -> dict[str, int]:
        """
        Load records from all sources in one call.

        Args:
            source_records: dict mapping source_name -> list of records
            partition_date: ISO date string, defaults to today

        Returns:
            dict mapping source_name -> rows loaded
        """
        results = {}
        for source_name, records in source_records.items():
            rows_loaded = self.load(source_name, records, partition_date)
            results[source_name] = rows_loaded
        return results

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
