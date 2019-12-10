import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
p = beam.Pipeline(options=PipelineOptions())

input_file = 'C:\\Users\\apar.prasanna\\Desktop\\dataflowpoc\\sample_data.csv'
outfile = 'C:\\Users\\apar.prasanna\\Desktop\\dataflowpoc\\sample_data_1'
(p
 | "READING_FILE" >> beam.io.ReadFromText(input_file,skip_header_lines=True)
 |  "writing_file" >> beam.io.WriteToText(outfile,file_name_suffix='.csv',shard_name_template='')
 )

p.run()