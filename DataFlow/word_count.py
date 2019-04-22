import apache_beam as beam
import sys
import re
import logging
input_file = '../source_files/sample-text.txt'

p = beam.Pipeline(argv=sys.argv)

output = '../output_files/wordcount_output.txt'

def print_line(lines):


    print(lines)
    yield lines



(p
 | "read_file" >> beam.io.ReadFromText(input_file)
 | "words" >> beam.FlatMap(lambda line:re.findall(r'[A-Za-z\']+', line ))
 | "Eachcountelement" >> beam.combiners.Count.PerElement()
 | "word_count" >> beam.Map(lambda word : '%s : %s' %(word[0],word[1]))
 | "load_data_outputfile" >> beam .io.WriteToText(output)


 )
p.run()

