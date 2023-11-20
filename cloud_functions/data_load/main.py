from google.cloud import bigquery
import functions_framework
import json
import os

bucket_name = os.getenv("BUCKET_NAME")
project_id = os.getenv("PROJECT_ID")
dataset_name = os.getenv("DATASET_NAME")
table_name = os.getenv("TABLE_NAME")
csvfile_name = "*.csv"

bq_client = bigquery.Client()


def logging(severity, message):
    # Build structured log messages as an object.
    global_log_fields = {}
    headers = ''
    request_is_defined = "request" in globals() or "request" in locals()
    if request_is_defined:
        trace_header = headers.get("X-Cloud-Trace-Context")

        if trace_header and project_id:
            trace = trace_header.split("/")
            global_log_fields[
                "logging.googleapis.com/trace"
            ] = f"projects/{project_id}/traces/{trace[0]}"

    # Complete a structured log entry.
    entry = dict(
        severity=severity,
        message=message,
        # Log viewer accesses 'component' as jsonPayload.component'.
        component="arbitrary-property",
        **global_log_fields,
    )

    print(json.dumps(entry))


# Register an HTTP function with the Functions Framework
@functions_framework.http
def load_data(request):
    schema_file = "schema.json"
    # https://cloud.google.com/bigquery/docs/schemas?hl=ja#specifying_a_schema_file_when_you_load_data
    schema = bq_client.schema_from_json(schema_file)
    uri = "gs://" + bucket_name + "/" + csvfile_name

    table_id = f"{project_id}.{dataset_name}.{table_name}"
    logging("DEBUG", f"TABLE: {table_id}.")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig#google_cloud_bigquery_job_LoadJobConfig_create_disposition
        create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = bq_client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
        )
    load_job.result()

    if load_job.state == "DONE":
        logging("INFO", "Data loaded successfully.")
    else:
        logging("ERROR", "Error: loading data.")

    return "OK"
