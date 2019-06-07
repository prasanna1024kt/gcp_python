import requests
from pandas.io.json import json_normalize
import json
import errno
import os
import logging
# from hooks.gcs_hook import CustomGcsHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from hooks.CustomGsUtilHook import CustomGSUtilHook
# from hooks.timeit_hook import measure_execution_time
# import myutils.utils as myfunctions
from airflow.exceptions import AirflowException




class MoatOperator():

    def __init__(self,
                 url,
                 #conn_id,
                 #authentication_file,
                 dimensions,
                 columns= None,
                 output_file_path=None,
                 bucket_name=None,
                 #gcs_location,
                 *args, **kwargs):


        self.url = url
        #self.conn_id=conn_id
        #self.authentication_file = authentication_file
        self.dimensions = dimensions
        self.columns = columns
        self.output_file_path = output_file_path
        self.bucket_name=bucket_name
       # self.gcs_location=gcs_location



    def execute(self):
#         with open(self.authentication_file, 'r') as auth:
#             json_data = json.load(auth)
#             ACCESS_TOKEN = "zz5ENvhyR96i61bcuqnRQIysaMqtiUrvDfGRwLTv"
#             access_token = ACCESS_TOKEN
# #            client_id = json_data['CLIENT_ID']
# #            client_secret = json_data['CLIENT_SCERET']

        try:
            ## calling the moat API
#            api_call = requests.get(self.url % (self.dimensions), auth=(client_id, client_secret))
            ACCESS_TOKEN = "zz5ENvhyR96i61bcuqnRQIysaMqtiUrvDfGRwLTv"
            access_token = ACCESS_TOKEN
            auth_header = 'Bearer {}'.format(access_token)
            api_call = requests.get(self.url % (self.dimensions), headers={'Authorization': auth_header})
            logging.info(self.url % (self.dimensions))
            # loading api content into json format
            json_data = json.loads(api_call.content)
            print(json_data)
            # cols = self.columns.split(',')
            # data_extraction = json_normalize(json_data['results']['details'])[cols]
            # logging.info(json_normalize(json_data['results']['details'])[cols])
            # if not os.path.exists(os.path.dirname(self.output_file_path)):
            #     try:
            #         os.makedirs(os.path.dirname(self.output_file_path))
            #     except OSError as exce:
            #         if exce.errno != errno.EEXIST:
            #             raise AirflowException("error while creating the file")
            # data_extraction.to_csv(self.output_file_path, index=False, encoding="utf-8")
            # #moving output file into GCS
            # bucket = self.bucket_name
            # logging.info("bucket name: %s" %bucket)
            # object = self.gcs_location + (os.path.basename(self.output_file_path))
            # logging.info("gcs path : %s" %object )
            # # gcs_hook = CustomGcsHook(google_cloud_storage_conn_id=self.conn_id)
            # # gcs_hook.upload(bucket, object, self.output_file_path)

        except Exception, e:

            raise AirflowException ("Unexpected Error while copying from local to gcs : %s" %e)


# CREDENTIAL :
#  authentication_file: '/opt/.d2p_config/config/moat_new_authentication_file.json'

URL = 'https://api.moat.com/1/stats.json?start=20190603&end=20190603&columns=%s'
ACCESS_TOKEN = ""
# LOCAL :
#  filelocation : '/mnt/disks/airflow-tmp/moat_ads_{{ds}}/moat_ads_{{ds_nodash}}.csv'
# GCS_STAGING :
#     bucket_name : 'newsuk-datatech-prod'
#     location : 'gs://newsuk-datatech-prod/'
#     file_path : 'staging/moat_ads/{{ds}}/'
#     log_file :  'moat_ads_{{ds_nodash}}.csv'
# BIG_QUERY :
#     data_set_name: "moat_api"
#     table_name : "moat_ads_analysis"
#     stage_table : "moat_stage_ads_data_analysis"
#     query : "select *, cast('{{ds}}' as date) as start_date,cast('{{ds}}' as date) as end_date,cast('{{ts}}' as timestamp) as processed_time  from [newsuk-datatech-prod:moat_api.moat_stage_ads_data_analysis_{{ds_nodash}}]"

# REPORT_QUERY:
api_metrics = 'start,end,level1,level2,level3,slicer1,slicer2,x_y,os_browser,active_view_viewable_impressions,human_and_viewable,active_view_viewable_impressions_rate,human_and_viewable_perc,universal_interactions_percent,total_ad_dwell_time,active_exposure_time,active_exposure_time_hr,average_minute_audience,impressions_analyzed,in_view_impressions,in_view_percent,l_full_visibility_ots_1_sec,l_full_visibility_ots_1_sec_percent,active_in_view_time,universal_interaction_time,measurable_impressions'
output_columns= 'level1_id,level1_label,level2_id,level2_label,level3_id,level3_label,slicer1_id,slicer1_label,slicer2_id,slicer2_label,x_y,os_browser,active_view_viewable_impressions,human_and_viewable,active_view_viewable_impressions_rate,human_and_viewable_perc,universal_interactions_percent,total_ad_dwell_time,active_exposure_time,active_exposure_time_hr,average_minute_audience,impressions_analyzed,in_view_impressions,in_view_percent,l_full_visibility_ots_1_sec,l_full_visibility_ots_1_sec_percent,active_in_view_time,universal_interaction_time,measurable_impressions'

objt =MoatOperator(URL,api_metrics)
objt.execute()