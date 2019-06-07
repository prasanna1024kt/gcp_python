import requests
import json
import os
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.gcs_hook import CustomGcsHook
from hooks.timeit_hook import measure_execution_time


class AppNexusAPIOperator(BaseOperator):
    template_fields = ("outputCsvFile", "reportURL", "dest_path", "filters","begins_date","term_date","file_name")

    @apply_defaults
    def __init__(self,
                 authUrl,
                 authFile,
                 reportURL,
                 downloadURL,
                 outputCsvFile,
                 file_name,
                 report_name,
                 columns,
                 gcs_bucket,
                 gcs_conn_id,
                 dest_path,
                 begins_date=None,
                 term_date=None,
                 gsutil_options='-rR',
                 filters=None,
                 *args, **kwargs
                 ):
        super(AppNexusAPIOperator, self).__init__(*args, **kwargs)
        self.authUrl = authUrl
        self.authFile = authFile
        self.reportURL = reportURL
        self.downloadURL = downloadURL
        self.outputCsvFile = outputCsvFile
        self.file_name= file_name
        self.report_name= report_name
        self.columns = columns
        self.begins_date = begins_date
        self.gcs_conn_id=gcs_conn_id
        self.gcs_bucket=gcs_bucket
        self.term_date = term_date
        self.dest_path = dest_path
        self.gsutil_options = gsutil_options
        self.filters = filters

    @measure_execution_time
    def execute(self,context):

        gcs_hook = CustomGcsHook(google_cloud_storage_conn_id=self.gcs_conn_id)
        auth = open(self.authFile)
        session = requests.Session()
        bucket = self.gcs_bucket
        try:

            response = session.post(self.authUrl, data=auth)

            logging.info("auth response %s"%response.status_code)
            if response.status_code == 200:
                logging.info("authenticated successfully")
            cookies = session.cookies
            logging.info("cookies details : %s" %cookies)

            reports = {
                "report":
                    {
                        "report_type": self.report_name,
                        "columns": self.columns,
                        "start_date": self.begins_date,
                        "end_date": self.term_date,
                        "format": "csv"
                    }
            }
            logging.info("report requested has : %s" % reports)

            try:

                response_id_extraction = requests.post(self.reportURL, cookies=cookies, data=json.dumps(reports))
                if response_id_extraction.status_code == 200:
                    logging.info("Extracted report id response details : %s" % response_id_extraction)
                    logging.info("Extracted report id response content : %s" % response_id_extraction.content)
                    json_data = json.loads(response_id_extraction.content)
                    report_id = json_data['response']['report_id']
                    logging.info("report id of the report %s" % (report_id))
                    params = (
                        ('id', report_id),
                    )
                    extract_data = requests.get(self.downloadURL, params=params,
                                                cookies=session.cookies.get_dict())
                    logging.info("temp output file path : %s" % self.outputCsvFile)
                    try:
                        if not os.path.exists(os.path.dirname(self.outputCsvFile)):
                            os.makedirs(os.path.dirname(self.outputCsvFile))

                        outputFile = self.outputCsvFile + self.file_name
                        logging.info("file name is %s" %self.file_name)
                        with open(outputFile, "wb") as output_file:
                            output_file.write(extract_data.content)
                        logging.info("bucket name: %s" % bucket)
                        object = self.dest_path + self.file_name
                        logging.info("object details : %s" %object)
                        logging.info("output file details : %s " %outputFile)
                        logging.info("File moving from local to GCS ")

                        gcs_hook.upload(bucket, object, outputFile)

                    except:
                        logging.info("Permission denied to create temp file")
            except:
                logging.info(response_id_extraction.status_code)

        except:
            logging.info("authentication failed ")




