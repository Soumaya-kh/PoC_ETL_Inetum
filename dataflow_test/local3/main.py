# This is an Apache Beam training Python script.
# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import argparse
import logging

import apache_beam as beam
from apache_beam.coders.coders import Coder
from datetime import datetime

import nltk
from nltk.corpus import stopwords
import re

from apache_beam.options.pipeline_options import PipelineOptions


class ISOCoder(Coder):
    """A coder used for reading and writing strings as ISO-8859-1."""

    def encode(self, value):
        return value.encode('iso-8859-1')

    def decode(self, value):
        return value.decode('iso-8859-1')

    def is_deterministic(self):
        return True


class FilterByLang(beam.DoFn):
    def process(self, element):
        _, date_str, ngram, lang, _, _, _, _, _ = element.split(';')
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %Z')
        if lang == 'en':
            yield ((date.strftime('%Y-%m'), ngram), _)


nltk.download('stopwords')
stopwords_english = set(stopwords.words('english'))
#print(stopwords_english)

class FilterStopwords(beam.DoFn):
    def process(self, element):
        (_, ngram), _ = element
        if ngram.lower() not in stopwords_english:
            if not(re.compile(r'[\d\W]+\w*').match(ngram)):
                # \d : digits [0-9]
                # \W : not in [a-zA-Z0-9], i.e. special character
                # \w digits and alphabet [a-zA-Z0-9]
                # + 1 or more
                # * 0, 1 or more
                yield element


class CountNgrams(beam.DoFn):
    def process(self, element):
        (date, ngram), counts = element
        yield f'{date};{ngram};{counts}'


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
        (p | 'ReadFile' >> beam.io.ReadFromText(known_args.input, coder=ISOCoder(), skip_header_lines=1)
         | 'FilterByLang' >> beam.ParDo(FilterByLang())
          | 'FilterStopWords' >> beam.ParDo(FilterStopwords())
         | 'CountNgrams' >> beam.combiners.Count.PerKey()
         | 'FormatOutput' >> beam.ParDo(CountNgrams())
         # | beam.Map(print))
         | 'WriteFile' >> beam.io.WriteToText(known_args.output, file_name_suffix='.csv', header='date;ngram;count'))
