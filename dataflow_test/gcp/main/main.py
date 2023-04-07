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

from apache_beam.io.gcp.internal.clients import bigquery


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
        if element['lang'] == 'en':
            yield ((element['date'].strftime('%Y-%m'), element['ngram']), 1)


class FilterStopwords(beam.DoFn):
    def __init__(self, arg_stopwords_english):
        self.stopwords_english = arg_stopwords_english

    def process(self, element):
        (_, ngram), _ = element
        if ngram.lower() not in self.stopwords_english:
            if not(re.compile(r'[\d\W]+\w*').match(ngram)):
                # \d : digits [0-9]
                # \W : not in [a-zA-Z0-9], i.e. special character
                # \w digits and alphabet [a-zA-Z0-9]
                # + 1 or more
                # * 0, 1 or more
                yield element


class Format_to_dict(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        (date_str, ngram), counts = element
        date = datetime.strptime(date_str, "%Y-%m")
        yield {'date': date, 'ngram': ngram, 'count': counts}


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        dest='project',
        # default='NBED',
        help='project ID'
    )
    parser.add_argument(
        '--dataset',
        dest='dataset',
        # default='NBED',
        help='Dataset ID'
    )
    parser.add_argument(
        '--tablein',
        dest='input_table',
        # default='NBED',
        help='Input Table'
    )
    parser.add_argument(
        '--tableout',
        dest='output_table',
        # default='NBED',
        help='Output table to write results to.'
    )

    known_args, beam_args = parser.parse_known_args()
    beam_args.extend(['--project=' + known_args.project])
    # beam_args.extend(['--autoscaling_algorithm=' + 'THROUGHPUT_BASED'])
    # beam_args.extend(['--max_num_workers=1000'])
    # beam_args.extend(['--num_workers=3'])

    # for the input table
    input_table_spec = bigquery.TableReference(
        projectId=known_args.project,
        datasetId=known_args.dataset,
        tableId=known_args.input_table)

    # for the output table
    output_table_spec = bigquery.TableReference(
        projectId=known_args.project,
        datasetId=known_args.dataset,
        tableId=known_args.output_table)

    table_schema = 'date:DATETIME,ngram:STRING,count:INTEGER'

    # table_schema = {
    #     'fields': [{
    #         'name': 'date', 'type': 'DATETIME', 'mode': 'REQUIRED'
    #     }, {
    #         'name': 'ngram', 'type': 'STRING', 'mode': 'REQUIRED'
    #     }, {
    #         'name': 'count', 'type': 'INTEGER', 'mode': 'REQUIRED'
    #     }]
    # }

    beam_options = PipelineOptions(beam_args)

    nltk.download('stopwords')
    stopwords_english = set(stopwords.words('english'))

    with beam.Pipeline(options=beam_options) as p:
        (p | 'ReadData' >> beam.io.ReadFromBigQuery(table=input_table_spec)
         # Each row is a dictionary where the keys are the BigQuery columns
         | 'FilterByLang' >> beam.ParDo(FilterByLang())
         | 'FilterStopWords' >> beam.ParDo(FilterStopwords(stopwords_english))
         | 'CountNgrams' >> beam.combiners.Count.PerKey()
         | 'FormatOutput' >> beam.ParDo(Format_to_dict())
         # | 'WriteFile' >> beam.io.WriteToText('gs://dflowtestbucket/filtered_data_webngrams', file_name_suffix='.txt')
         # | beam.Map(print))
         | 'WriteData' >> beam.io.WriteToBigQuery(
                    output_table_spec,
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
