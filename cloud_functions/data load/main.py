from google.cloud import bigquery
from google.cloud import storage
import functions_framework
import json
from collections import OrderedDict
import os

project_id = os.getenv('PROJECT_ID')  
bucket_name = os.getenv('BUCKET_NAME')
csvfile_name = '*.csv'

cs_client = storage.Client()
bq_client = bigquery.Client()

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

# Register an HTTP function with the Functions Framework
@functions_framework.http
def load_data(request):
    if csvfile_name.startswith('old/') == False and csvfile_name.endswith('csv') == True:
        confile_name = 'bigquery.conf'
        jsonfile_name = 'schema.json'
        #confファイル読込
        conf_file_text = download_text(cs_client, bucket_name, confile_name)
        lines = conf_file_text.splitlines()
        skipleading_rows = ''
        project_id = ''
        dataset_name = ''
        table_id = ''
        for line in lines:
            if ('SkipReadingRows' in line):
                skipleading_rows = line.rsplit('=',1)[1].strip()
            if ('ProjectId' in line):
                project_id = line.rsplit('=',1)[1].strip()
            if ('DatasetName' in line):
                dataset_name = line.rsplit('=',1)[1].strip()
            if ('TableName' in line):
                table_name = line.rsplit('=',1)[1].strip()
        #table_idの作成
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        logging(f"TABLE: {table_id}")
        #jsonファイル読込
        json_file_text = download_text(cs_client, bucket_name, jsonfile_name)
        json_load = json.loads(json_file_text, object_pairs_hook=OrderedDict)
        job_config = csvloadjobjsonconfig(json_load,skipleading_rows)

        #Bigqueryにデータロード
        uri = 'gs://' + bucket_name + '/' + csvfile_name
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

        if load_job.state == "DONE":
            logging(f"Data loaded successfully.")
        else:
            logging(f"Error: loading data.")

    return 'OK'

#storageにある設定ファイルをダウンロード
def download_text(cs_client, bucket, filepath):
    bucket = cs_client.get_bucket(bucket)
    blob = storage.Blob(filepath, bucket)
    bcontent = blob.download_as_string()
    try:
        return bcontent.decode("utf8")
    except UnicodeError as e:
        logging(f"catch UnicodeError: {e}")  
    return ""

#csvロード用configの作成
def csvloadjobjsonconfig(jsonload, skipleadingrows):
    schema = []
    for schematmp in jsonload:
        schema.append(bigquery.SchemaField(schematmp['name'], schematmp['type'],mode=schematmp.get('mode',""),description=schematmp.get('description',"")))        
    job_config = bigquery.LoadJobConfig()
    # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig#google_cloud_bigquery_job_LoadJobConfig_create_disposition
    job_config.create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = skipleadingrows
    job_config.schema = schema 
    return job_config