import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import json
import os

project_id = os.getenv('PROJECT_ID')  
bucket_name = os.getenv('BUCKET_NAME')
sqlfile_name = 'bigquery.sql'

bq_client = bigquery.Client()
cs_client = storage.Client()

def logging(message):
    # Build structured log messages as an object.
    global_log_fields = {}

    request_is_defined = "request" in globals() or "request" in locals()
    if request_is_defined and request:
        trace_header = request.headers.get("X-Cloud-Trace-Context")

        if trace_header and project_id:
            trace = trace_header.split("/")
            global_log_fields[
                "logging.googleapis.com/trace"
            ] = f"projects/{project_id}/traces/{trace[0]}"

    # Complete a structured log entry.
    entry = dict(
        severity="NOTICE",
        message=message,
        # Log viewer accesses 'component' as jsonPayload.component'.
        component="arbitrary-property",
        **global_log_fields,
    )

    print(json.dumps(entry))

@functions_framework.http
def analyse_data(request):
  bucket = cs_client.get_bucket(bucket_name)
  blob = bucket.blob(sqlfile_name)
  query = blob.download_as_text()

  job_config = bigquery.QueryJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
  )
  query_job = bq_client.query(
    query,
    job_config=job_config
  )

  query_job.result()
  logging(f"Data queried successfully.")
  return 'OK'