import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions,StandardOptions,GoogleCloudOptions
from apache_beam.io.textio import ReadFromText,WriteToText


input_file = ''
output_file = ''

options = PipelineOptions()
gcloud_options = options.view_as(GoogleCloudOptions)
gcloud_options.job_name='dataflow_data_job'

#local runner
options.view_as(StandardOptions).runner = 'direct'

