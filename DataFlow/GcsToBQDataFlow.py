from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
PROJECT=''
BUCKET='sample_data_load'
class DataIngestion:
     def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('state', 'gender', 'year', 'name', 'number', 'created_date'),
                values))
        return row


def run(argv=None):
    parser = argparse.ArgumentParser()
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=sample_data_load_bq',
        '--save_main_session',
        '--staging_location=gs://{0}/sample_data_load/'.format(BUCKET),
        '--temp_location=gs://{0}/sample_data_load/'.format(BUCKET),
        '--runner=DataflowRunner'
    ]

    input='location'
    # parser.add_argument('--output',
    #                     dest='output',
    #                     required=False,
    #                     help='Output BQ table to write results to.',
    #                     default='ibx-ops-dev:dataflow_poc.SampleDataLoad')
    known_args, pipeline_args = parser.parse_known_args(argv)
    data_ingestion = DataIngestion()
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read from a File' >> beam.io.ReadFromText(input,
                                                  skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
                beam.io.BigQuerySink(known_args.output,
                    schema='state:STRING,gender:STRING,year:STRING,name:STRING,'
                           'number:STRING,created_date:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
