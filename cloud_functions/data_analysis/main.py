import functions_framework
from google.cloud import bigquery
import json
import os

project_id = os.getenv("PROJECT_ID")
dataset_name = os.getenv("DATASET_NAME")
table_name = os.getenv("TABLE_NAME")

bq_client = bigquery.Client()


def logging(severity, message):
    # Build structured log messages as an object.
    global_log_fields = {}
    headers = ""
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


@functions_framework.http
def analyse_data(request):
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    sql_file_path = "bigquery.sql"
    with open(sql_file_path, "r") as sqlfile:
        query = sqlfile.read()

    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    query_job = bq_client.query(query, job_config=job_config)
    query_job.result()

    table = bq_client.get_table(table_id)
    logging("DEBUG", f"Loaded {table.num_rows} rows to table {table_id}.")
    if query_job.state == "DONE":
        logging("INFO", "Data analysed successfully.")
    else:
        logging("ERROR", "Error: analysing data.")
    return "OK"
