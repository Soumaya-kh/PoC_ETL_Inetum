# This is an Apache Beam training Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import argparse
import logging

import apache_beam as beam
from apache_beam.coders.coders import Coder

from apache_beam.options.pipeline_options import PipelineOptions


class ISOCoder(Coder):
    """A coder used for reading and writing strings as ISO-8859-1."""

    def encode(self, value):
        return value.encode('iso-8859-1')

    def decode(self, value):
        return value.decode('iso-8859-1')

    def is_deterministic(self):
        return True


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        # default='gs://dataflow-samples/shakespeare/kinglear.txt',
        default='raw_data_webngrams.csv',
        help='Input file to process.'
    )
    parser.add_argument(
        '--output',
        dest='output',
        default='filtered_data_webngrams',
        required=False,
        help='Output file to write results to.'
    )
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline() as p:
        (p | beam.io.ReadFromText(known_args.input, coder=ISOCoder(), skip_header_lines=1)
         | beam.Map(lambda x: x.split(';'))
         | beam.Filter(lambda ligne: ligne[3] == "en")
         # | beam.Map(print)
         | beam.io.WriteToText(known_args.output, file_name_suffix='.csv'))
