
"""
implementing the grep in dataflow using python
"""

import apache_beam as beam

import re
import sys


def search_item(line,item):

    if re.match(r'^' + re.escape(item),line):
        yield line

p = beam.Pipeline(argv=sys.argv)
input_file = '../pubsub/*.py'

output_file = './output_file.txt'
item ='import'

(p
 | "input" >> beam.io.ReadFromText(input_file)
 | "filter" >> beam.FlatMap(lambda line:search_item(line,item))
 | "write"  >> beam.io.WriteToText(output_file)

 )

p.run()
