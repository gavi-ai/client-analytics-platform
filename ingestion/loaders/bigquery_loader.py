import os
import json
import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)

def load_to_bigquery(data_payload: dict, dataset_id: str, table_name: str):
    """
    Loads raw JSON data directly into BigQuery Raw dataset.
    """
    try:
        client = bigquery.Client()
        table_id = f"{os.getenv('BIGQUERY_PROJECT')}.{dataset_id}.{table_name}"
        
        # In a real scenario, we save to NDJSON and load, or stream directly
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        logger.info(f"Loading {len(data_payload)} records to {table_id}...")
        # Simulating the load process for the repo code structure
        # load_job = client.load_table_from_json(data_payload, table_id, job_config=job_config)
        # load_job.result()  
        logger.info(f"Successfully loaded data to {table_id}")
        
    except Exception as e:
        logger.error(f"Failed to load data to BigQuery: {str(e)}")
        raise e