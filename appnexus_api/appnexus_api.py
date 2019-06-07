import os
from datetime import datetime, timedelta
import yaml
import json
from airflow import DAG
from airflow.models import Variable
from hooks.slack_hook import CusotmSlackHook
from operators.custom_slack_operators import CustomSlackPostOperator
from operators.CustomAppnexusOperator import AppNexusAPIOperator
from operators.custom_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from myutils import utils


VARIABLES = Variable()
ENV = VARIABLES.get(key='env')
DAG_NAME = "appnexus_api_reports.0.0.1"
DAG_CONFIG = yaml.load(open(os.path.join(os.path.dirname(__file__),
                                         "conf/appnexus_%s.yaml" % ENV.lower())))
DATE_OBJ = datetime.strptime(DAG_CONFIG['START_DATE'], "%d/%m/%Y %H:%M:%S")
START_DATE = datetime(DATE_OBJ.year, DATE_OBJ.month, DATE_OBJ.day, DATE_OBJ.hour)
SCHEDULE_INTERVAL = DAG_CONFIG['INTERVAL']
DATE_HOUR = '{{ti.xcom_pull(key="date_hour", task_ids="date_hour")}}'
DATE = '{{ti.xcom_pull(key="date", task_ids="date_hour")}}'
DATE_NODASH = '{{ti.xcom_pull(key="date_nodash", task_ids="date_hour")}}'
DATE_HOUR_MINUTE = '{{ti.xcom_pull(key="date_hour_minute", task_ids="date_hour")}}'
PROJECT_ID=DAG_CONFIG['PROJECT_ID']
REPORT_URL = DAG_CONFIG['REPORT_URL']
DOWNLOAD_URL = DAG_CONFIG['DOWNLOAD_URL']
AUTHENTICATION_FILE=DAG_CONFIG['authentication_file']
AUTH_URL=DAG_CONFIG['AUTH_URL']
GCS_CONN_ID = "GCS_%s" % ENV.upper()
#GCS_CONN_ID = "GCS_DEV"
SLACK_CONN_ID = 'slack_%s' % ENV.lower()

DEV_EMAIL_LIST = [
                  'sangeetha.treesa@news.co.uk',
                  'prasanna.kumar@@news.co.uk',
                  'manojkumar.vadivel@news.co.uk',
                  'maheshkumar.patlo@news.co.uk'
                  ]

MAP_SLACK_ATTACHMENTS = """[
    {
        "color": "#36a64f",
        "title": "{{var.value.env}} - Airflow Job %s",
        "title_link": "{{var.value.domain}}/admin/airflow/tree?dag_id=appnexus_api_reports.0.0.1",
        "fields": [
            {
                "title": "Dag Id",
                "value": "%s",
                "short": "True"
            },
            {
                "title": "Execution Date",
                "value": "{{ds}}",
                "short": "True"
            }
        ]
    }
  ]"""


def slack_failure_notification(context):
    """Slack error notification when job fails"""
    domain = VARIABLES.get(key='domain')
    slack_failure_channel = VARIABLES.get(key='slack_failure_channel')
    attachment_config = {}
    attachment_config['color'] = 'danger'
    attachment_config['title'] = "Airflow job Failed"
    attachment_config['title_link'] = "%s/admin/" \
                                      "airflow/tree?dag_id=appnexus_api_reports.0.0.1" % domain
    slack_connection = CusotmSlackHook(conn_id=SLACK_CONN_ID)
    slack_connection.post_attachment(config=attachment_config,
                                     context=context,
                                     channel="#%s" % slack_failure_channel)

DEFAULT_ARGUMENTS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": START_DATE,
    "schedule_interval": SCHEDULE_INTERVAL,
    "email": DEV_EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_notification,
    "sla": timedelta(hours=5)
}

DAG_ID = DAG(DAG_NAME,
             start_date=START_DATE,
             schedule_interval=SCHEDULE_INTERVAL,
             default_args=DEFAULT_ARGUMENTS)

SLACK_MESSAGE_START = CustomSlackPostOperator(
        task_id="job_initiate_notification",
        text="Job Status Summary",
        icon_url="http://static1.squarespace.com/static/54e2173fe4b0f4a6ba359926/"
                 "556ab33ee4b0ffd5dc069d01/556ab3e7e4b0c63251e14f8a/"
                 "1433056231884/News+UK+Logo.png?format=500w",
        token="{{var.value.slack_token}}",
        channel="{{var.value.slack_channel}}",
        attachments=json.loads(MAP_SLACK_ATTACHMENTS % (DAG_NAME, "started successfully")),
        dag=DAG_ID
    )
for line in DAG_CONFIG['REPORTS']:

     APPNEXUS_API_CALL = AppNexusAPIOperator(
        task_id='appnexus_%s_report' %(line['report']),
        dag = DAG_ID,
        authUrl = AUTH_URL,
        reportURL = REPORT_URL,
        downloadURL = DOWNLOAD_URL,
        authFile = AUTHENTICATION_FILE,
        report_name = line['report'],
        columns = line['columns'],
        begins_date = line['start_date'] + ' ' + '00:00:00' ,
        term_date = line['end_date'] + ' ' + '00:00:00' ,
        outputCsvFile = line['file_location'],
        file_name=line['local_file_name']%line['report'],
        gcs_conn_id=GCS_CONN_ID,
        gcs_bucket=line['bucket_name'],
        dest_path =line['file_path']

     )
     GCS_TO_BQ = GoogleCloudStorageToBigQueryOperator(
         task_id="%s_gcs_to_bq_load" % (line['report']),
         dag=DAG_ID,
         bigquery_conn_id=GCS_CONN_ID,
         google_cloud_storage_conn_id=GCS_CONN_ID,
         bucket=utils.obj_bucket(line['location']),
         source_objects=[utils.obj_path(line['location'] + line['file_path'] + line['log_file'])],
         project_id=DAG_CONFIG['PROJECT_ID'],
         big_query_table_name=line['table_name'],
         big_query_data_set_name=line['dataset'],
         schema_fields=json.loads(open(os.path.join(os.path.dirname(__file__), line['table_schema'])).read()),
         source_format="CSV",
         create_disposition="CREATE_IF_NEEDED",
         skip_leading_rows=1,
         write_disposition="WRITE_TRUNCATE",
         maxBadRecords=0,
         field_delimiter=',',
         max_id_key=False,
         encoding='UTF-8',
         flag_per_directory=False,
         ignore_unknown_values=True,
         allow_jagged_rows=False,
     )
     SLACK_MESSAGE_START.set_downstream(APPNEXUS_API_CALL)
     APPNEXUS_API_CALL.set_downstream(GCS_TO_BQ)


SLACK_MESSAGE_COMPLETE = CustomSlackPostOperator(
        task_id="job_Complete_notification",
        text="Job Status Summary",
        icon_url="http://static1.squarespace.com/static/54e2173fe4b0f4a6ba359926/"
                     "556ab33ee4b0ffd5dc069d01/556ab3e7e4b0c63251e14f8a/"
                     "1433056231884/News+UK+Logo.png?format=500w",
        token="{{var.value.slack_token}}",
        channel="{{var.value.slack_channel}}",
        attachments=json.loads(MAP_SLACK_ATTACHMENTS % (DAG_NAME, "Completed successfully")),
        dag=DAG_ID
)

GCS_TO_BQ.set_downstream(SLACK_MESSAGE_COMPLETE)