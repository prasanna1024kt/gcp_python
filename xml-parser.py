# pylint: disable=F0401,ungrouped-imports
import re
import json
import logging
import datetime
import xmltodict
from airflow import DAG
from hooks.custom_bq_hook import CustomBQHook
from airflow.models import BaseOperator
from hooks.web_crawler_hook import WebXMLCrawling
from airflow.utils.file import TemporaryDirectory
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class XMLParseImageUrl(BaseOperator):
    """
    Parse image url from given xml content
    """

    def __init__(self,
                 url,
                 local,
                 local_file_name,
                 bq_conn_id,
                 bq_project_id,
                 bq_data_set_name,
                 bq_table_name,
                 bq_check_query,
                 *args,
                 **kwargs):
        super(XMLParseImageUrl, self).__init__(*args, **kwargs)
        self.url = url
        self.local = local
        self.local_file_name = local_file_name
        self.bq_conn_id = bq_conn_id
        self.bq_project_id = bq_project_id
        self.bq_data_set_name = bq_data_set_name
        self.bq_table_name = bq_table_name
        self.bq_check_query = bq_check_query

    def check_article_id_exists(self):
        bq_hook = CustomBQHook()
        response = bq_hook.bq_query_table(self.bq_project_id,
                                          query=self.bq_check_query,
                                          format='json')
        if not response:
            response = ''
        response = response[response.rfind('[{'):]
        if not response or not response.strip():
            response = '[]'
        x = pd.read_json(response)
        pub_date = pd.DataFrame(x)
        return [int(row['article_id']) for row in json.loads(response)], pub_date

    def execute(self, context):

        xml_hook = WebXMLCrawling()
        xml_dom = xml_hook.parse_xml(self.url)
        xml_tag = xml_dom.getElementsByTagName('url')
        bq_hook = CustomBQHook()

        schema = [{"type": "STRING", "name": "article_id"},
                  {"type": "STRING", "name": "article_location"},
                  {"type": "STRING", "name": "image_location"},
                  {"type": "STRING", "name": "keywords"},
                  {"type": "STRING", "name": "publication_date"},
                  {"type": "STRING", "name": "publisher"},
                  {"type": "STRING", "name": "title"},
                  {"type": "TIMESTAMP", "name": "updated_time"}]

        with TemporaryDirectory(prefix='airflow') as tmp_dir:
            schema_path = "%s/%s" % (tmp_dir, "_schema.json")
            with open(schema_path, "w") as file_schema_object:
                json.dump(schema, file_schema_object)

            bq_hook.create_dataset_if_missing(self.bq_project_id,
                                              dataset_name=self.bq_data_set_name,
                                              data_location='EU')

            bq_hook.create_table_if_missing(project_id=self.bq_project_id,
                                            dataset_name=self.bq_data_set_name,
                                            table_name=self.bq_table_name,
                                            schema_path=schema_path)

        article_id_list, pub_date = self.check_article_id_exists()
        write_headers = True
        data_frame = pd.DataFrame(columns=['article_id', 'article_location', 'image_location', 'keywords',
                                                   'publication_date', 'publisher', 'title', 'updated_time'])

        for loc in xml_tag:
            xml_url_section = xmltodict.parse(loc.toxml())
            article_id = int(re.search('/(\d+)/', xml_url_section['url']['loc']).groups()[0])
            data1 = {
                'article_id': [str(article_id)],
                'article_location': [xml_url_section['url']['loc']],
                'title': [xml_url_section['url']['news:news']['news:title']],
                'publication_date':
                    [xml_url_section['url']['news:news']['news:publication_date']],
                'publisher': [xml_url_section['url']['news:news']['news:publication']['news:name']],
                'image_location':
                    [xml_url_section['url']['image:image']['image:loc']],
                'keywords': [xml_url_section['url']['news:news']['news:keywords']],
                'updated_time': datetime.datetime.now().isoformat()}
            data1 = pd.DataFrame(data1)
            data_frame1 = data1[['article_id', 'publication_date']]
            publication_date_old = str(pub_date.loc[pub_date['article_id'] == article_id]['publication_date'])
            publication_date_new = str(data_frame1.loc[data_frame1['article_id'] == article_id]['publication_date'])
            start_old = publication_date_old.find('T') - 10
            end_old = publication_date_old.find('T') + 9
            start_new = publication_date_new.find('T') - 10
            end_new = publication_date_new.find('T') + 9
            pub_date = pub_date[['article_id','publication_date']]
            if article_id not in article_id_list:
                data = {
                    'article_id': [str(article_id)],
                    'article_location': [xml_url_section['url']['loc']],
                    'title': [xml_url_section['url']['news:news']['news:title']],
                    'publication_date':
                        [xml_url_section['url']['news:news']['news:publication_date']],
                    'publisher': [xml_url_section['url']['news:news']['news:publication']['news:name']],
                    'image_location':
                        [xml_url_section['url']['image:image']['image:loc']],
                    'keywords': [xml_url_section['url']['news:news']['news:keywords']],
                    'updated_time': datetime.datetime.now().isoformat()}

                data = pd.DataFrame(data)
                data_frame = data_frame.append(data)

            if article_id in article_id_list and (publication_date_new[start_new:end_new]>
                                                         publication_date_old[start_old:end_old]) :
                data = {
                    'article_id': [str(article_id)],
                    'article_location': [xml_url_section['url']['loc']],
                    'title': [xml_url_section['url']['news:news']['news:title']],
                    'publication_date':
                        [xml_url_section['url']['news:news']['news:publication_date']],
                    'publisher': [xml_url_section['url']['news:news']['news:publication']['news:name']],
                    'image_location':
                        [xml_url_section['url']['image:image']['image:loc']],
                    'keywords': [xml_url_section['url']['news:news']['news:keywords']],
                    'updated_time': datetime.datetime.now().isoformat()}

                data = pd.DataFrame(data)
                data_frame = data_frame.append(data)

        data_frame.to_csv('%s%s_image_data.csv' % (self.local , self.local_file_name),
                          mode='w', index=False, header=True, sep='\t')

    '''def bq_insert_rows(self, project_id, dataset, table, rows):
        """
        Performs an insert into a BQ table.

        :param project: Project the dataset is in
        :param dataset: Dataset to insert into
        :param table: Table to insert into
        :param data: Dict of data to insert
        :return:
        """
        try:
            bq_hook = CustomBQHook()

            logging.info("Inserting into BQ %s:%s.%s: %s" % (project_id,
                                                             dataset,
                                                             table,
                                                             json.dumps(rows)))

            return bq_hook.bq_insert_rows(project_id, dataset, table, rows)
        except Exception, error_message:
            logging.error(error_message)
'''
