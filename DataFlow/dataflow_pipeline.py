import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import filesystem
import logging
from utils import myutils
input_file = 'C:\\Users\\apar.prasanna\\Desktop\\dataflowpoc\\sample_data.csv'
output_file_path = 'C:\\Users\\apar.prasanna\\Desktop\\dataflowpoc\\ptr.csv'

p = beam.Pipeline(options=PipelineOptions())

def dataparser(lines):
    import csv
    import pandas as pd

    from datetime import datetime, timedelta
    datetimeFormat = '%Y-%m-%d-%H.%M.%S.%f'
    reader = csv.reader(lines.split('\n'))
    line = lines.split(',')
    return line[84]

#
def printvalue(text):
    print(text)

lines = p | "reading_file" >> beam.io.ReadFromText(input_file)
data = lines| "data" >> beam.Map(dataparser)
datapar = lines | "parsedata" >> beam.Map(lambda  row : row)
ptr = datapar | "pri" >> beam.Map(printvalue)

p.run()
